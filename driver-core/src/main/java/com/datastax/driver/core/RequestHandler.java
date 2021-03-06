/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Handles a request to cassandra, dealing with host failover and retries on
 * unavailable/timeout.
 */
class RequestHandler implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final SessionManager manager;
    private final Callback callback;

    private final Iterator<Host> queryPlan;
    private final Query query;
    private volatile Host current;
    private volatile List<Host> triedHosts;
    private volatile HostConnectionPool currentPool;

    private volatile int queryRetries;
    private volatile ConsistencyLevel retryConsistencyLevel;

    private volatile Map<InetAddress, String> errors;

    private volatile boolean isCanceled;
    private volatile Connection.ResponseHandler connectionHandler;

    private final TimerContext timerContext;
    private final long startTime;

    public RequestHandler(SessionManager manager, Callback callback, Query query) {
        this.manager = manager;
        this.callback = callback;

        callback.register(this);

        this.queryPlan = manager.loadBalancingPolicy().newQueryPlan(query);
        this.query = query;

        this.timerContext = metricsEnabled()
                          ? metrics().getRequestsTimer().time()
                          : null;
        this.startTime = System.nanoTime();
    }

    private boolean metricsEnabled() {
        return manager.configuration().getMetricsOptions() != null;
    }

    private Metrics metrics() {
        return manager.cluster.manager.metrics;
    }

    public void sendRequest() {
        try {
            while (queryPlan.hasNext() && !isCanceled) {
                Host host = queryPlan.next();
                logger.trace("Querying node {}", host);
                if (query(host))
                    return;
            }
            setFinalException(null, new NoHostAvailableException(errors == null ? Collections.<InetAddress, String>emptyMap() : errors));
        } catch (Exception e) {
            // Shouldn't happen really, but if ever the loadbalancing policy returned iterator throws, we don't want to block.
            setFinalException(null, new DriverInternalError("An unexpected error happened while sending requests", e));
        }
    }

    private boolean query(Host host) {
        currentPool = manager.pools.get(host);
        if (currentPool == null || currentPool.isShutdown())
            return false;

        PooledConnection connection = null;
        try {
            // Note: this is not perfectly correct to use getConnectTimeoutMillis(), but
            // until we provide a more fancy to control query timeouts, it's not a bad solution either
            connection = currentPool.borrowConnection(manager.configuration().getSocketOptions().getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (current != null) {
                if (triedHosts == null)
                    triedHosts = new ArrayList<Host>();
                triedHosts.add(current);
            }
            current = host;
            connectionHandler = connection.write(this);
            return true;
        } catch (ConnectionException e) {
            // If we have any problem with the connection, move to the next node.
            if (metricsEnabled())
                metrics().getErrorMetrics().getConnectionErrors().inc();
            if (connection != null)
                connection.release();
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (BusyConnectionException e) {
            // The pool shoudln't have give us a busy connection unless we've maxed up the pool, so move on to the next host.
            if (connection != null)
                connection.release();
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (TimeoutException e) {
            // We timeout, log it but move to the next node.
            logError(host.getAddress(), "Timeout while trying to acquire available connection (you may want to increase the driver number of per-host connections)");
            return false;
        } catch (RuntimeException e) {
            if (connection != null)
                connection.release();
            logger.error("Unexpected error while querying " + host.getAddress(), e);
            logError(host.getAddress(), e.getMessage());
            return false;
        }
    }

    private void logError(InetAddress address, String msg) {
        logger.debug("Error querying {}, trying next host (error is: {})", address, msg);
        if (errors == null)
            errors = new HashMap<InetAddress, String>();
        errors.put(address, msg);
    }

    private void retry(final boolean retryCurrent, ConsistencyLevel newConsistencyLevel) {
        final Host h = current;
        this.retryConsistencyLevel = newConsistencyLevel;

        // We should not retry on the current thread as this will be an IO thread.
        manager.executor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (retryCurrent) {
                        if (query(h))
                            return;
                    }
                    sendRequest();
                } catch (Exception e) {
                    setFinalException(null, new DriverInternalError("Unexpected exception while retrying query", e));
                }
            }
        });
    }

    public void cancel() {
        isCanceled = true;
        if (connectionHandler != null)
            connectionHandler.cancelHandler();
    }

    @Override
    public Message.Request request() {

        Message.Request request = callback.request();
        if (retryConsistencyLevel != null) {
            org.apache.cassandra.db.ConsistencyLevel cl = ConsistencyLevel.toCassandraCL(retryConsistencyLevel);
            if (request instanceof QueryMessage) {
                QueryMessage qm = (QueryMessage)request;
                if (qm.consistency != cl)
                    request = new QueryMessage(qm.query, cl);
            }
            else if (request instanceof ExecuteMessage) {
                ExecuteMessage em = (ExecuteMessage)request;
                if (em.consistency != cl)
                    request = new ExecuteMessage(em.statementId, em.values, cl);
            }
        }
        return request;
    }

    private void setFinalResult(Connection connection, Message.Response response) {
        try {
            if (timerContext != null)
                timerContext.stop();

            ExecutionInfo info = current.defaultExecutionInfo;
            if (triedHosts != null)
            {
                triedHosts.add(current);
                info = new ExecutionInfo(triedHosts);
            }
            if (retryConsistencyLevel != null)
                info = info.withAchievedConsistency(retryConsistencyLevel);
            callback.onSet(connection, response, info, System.nanoTime() - startTime);
        } catch (Exception e) {
            callback.onException(connection, new DriverInternalError("Unexpected exception while setting final result from " + response, e), System.nanoTime() - startTime);
        }
    }

    private void setFinalException(Connection connection, Exception exception) {
        try {
            if (timerContext != null)
                timerContext.stop();
        } finally {
            callback.onException(connection, exception, System.nanoTime() - startTime);
        }
    }

    @Override
    public void onSet(Connection connection, Message.Response response, long latency) {
        Host queriedHost = current;
        try {
            if (connection instanceof PooledConnection)
                ((PooledConnection)connection).release();

            switch (response.type) {
                case RESULT:
                    setFinalResult(connection, response);
                    break;
                case ERROR:
                    ErrorMessage err = (ErrorMessage)response;
                    RetryPolicy.RetryDecision retry = null;
                    RetryPolicy retryPolicy = query.getRetryPolicy() == null
                                            ? manager.configuration().getPolicies().getRetryPolicy()
                                            : query.getRetryPolicy();
                    switch (err.error.code()) {
                        case READ_TIMEOUT:
                            assert err.error instanceof ReadTimeoutException;
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getReadTimeouts().inc();

                            ReadTimeoutException rte = (ReadTimeoutException)err.error;
                            ConsistencyLevel rcl = ConsistencyLevel.from(rte.consistency);
                            retry = retryPolicy.onReadTimeout(query, rcl, rte.blockFor, rte.received, rte.dataPresent, queryRetries);
                            break;
                        case WRITE_TIMEOUT:
                            assert err.error instanceof WriteTimeoutException;
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getWriteTimeouts().inc();

                            WriteTimeoutException wte = (WriteTimeoutException)err.error;
                            ConsistencyLevel wcl = ConsistencyLevel.from(wte.consistency);
                            retry = retryPolicy.onWriteTimeout(query, wcl, WriteType.from(wte.writeType), wte.blockFor, wte.received, queryRetries);
                            break;
                        case UNAVAILABLE:
                            assert err.error instanceof UnavailableException;
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getUnavailables().inc();

                            UnavailableException ue = (UnavailableException)err.error;
                            ConsistencyLevel ucl = ConsistencyLevel.from(ue.consistency);
                            retry = retryPolicy.onUnavailable(query, ucl, ue.required, ue.alive, queryRetries);
                            break;
                        case OVERLOADED:
                            // Try another node
                            logger.warn("Host {} is overloaded, trying next host.", connection.address);
                            logError(connection.address, "Host overloaded");
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            return;
                        case IS_BOOTSTRAPPING:
                            // Try another node
                            logger.error("Query sent to {} but it is bootstrapping. This shouldn't happen but trying next host.", connection.address);
                            logError(connection.address, "Host is boostrapping");
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            return;
                        case UNPREPARED:
                            assert err.error instanceof PreparedQueryNotFoundException;
                            PreparedQueryNotFoundException pqnf = (PreparedQueryNotFoundException)err.error;
                            PreparedStatement toPrepare = manager.cluster.manager.preparedQueries.get(pqnf.id);
                            if (toPrepare == null) {
                                // This shouldn't happen
                                String msg = String.format("Tried to execute unknown prepared query %s", pqnf.id);
                                logger.error(msg);
                                setFinalException(connection, new DriverInternalError(msg));
                                return;
                            }

                            logger.info("Query {} is not prepared on {}, preparing before retrying executing. "
                                      + "Seeing this message a few times is fine, but seeing it a lot may be source of performance problems",
                                        toPrepare.getQueryString(), connection.address);
                            String currentKeyspace = connection.keyspace();
                            String prepareKeyspace = toPrepare.getQueryKeyspace();
                            // This shouldn't happen in normal use, because a user shouldn't try to execute
                            // a prepared statement with the wrong keyspace set. However, if it does, we'd rather
                            // prepare the query correctly and let the query executing return a meaningful error message
                            if (prepareKeyspace != null && (currentKeyspace == null || !currentKeyspace.equals(prepareKeyspace)))
                            {
                                logger.trace("Setting keyspace for prepared query to {}", prepareKeyspace);
                                connection.setKeyspace(prepareKeyspace);
                            }

                            try {
                                connection.write(prepareAndRetry(toPrepare.getQueryString()));
                            } finally {
                                // Always reset the previous keyspace if needed
                                if (connection.keyspace() == null || !connection.keyspace().equals(currentKeyspace))
                                {
                                    logger.trace("Setting back keyspace post query preparation to {}", currentKeyspace);
                                    connection.setKeyspace(currentKeyspace);
                                }
                            }
                            // we're done for now, the prepareAndRetry callback will handle the rest
                            return;
                        default:
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            break;
                    }

                    if (retry == null)
                        setFinalResult(connection, response);
                    else {
                        switch (retry.getType()) {
                            case RETRY:
                                ++queryRetries;
                                if (logger.isTraceEnabled())
                                    logger.trace("Doing retry {} for query {} at consistency {}", new Object[]{ queryRetries, query, retry.getRetryConsistencyLevel()});
                                if (metricsEnabled())
                                    metrics().getErrorMetrics().getRetries().inc();
                                retry(true, retry.getRetryConsistencyLevel());
                                break;
                            case RETHROW:
                                setFinalResult(connection, response);
                                break;
                            case IGNORE:
                                if (metricsEnabled())
                                    metrics().getErrorMetrics().getIgnores().inc();
                                setFinalResult(connection, new ResultMessage.Void());
                                break;
                        }
                    }
                    break;
                default:
                    setFinalResult(connection, response);
                    break;
            }
        } catch (Exception e) {
            setFinalException(connection, e);
        } finally {
            if (queriedHost != null)
                manager.cluster.manager.reportLatency(queriedHost, latency);
        }
    }

    private Connection.ResponseCallback prepareAndRetry(final String toPrepare) {
        return new Connection.ResponseCallback() {

            @Override
            public Message.Request request() {
                return new PrepareMessage(toPrepare);
            }

            @Override
            public void onSet(Connection connection, Message.Response response, long latency) {
                // TODO should we check the response ?
                switch (response.type) {
                    case RESULT:
                        if (((ResultMessage)response).kind == ResultMessage.Kind.PREPARED) {
                            logger.trace("Scheduling retry now that query is prepared");
                            retry(true, null);
                        } else {
                            logError(connection.address, "Got unexpected response to prepare message: " + response);
                            retry(false, null);
                        }
                        break;
                    case ERROR:
                        logError(connection.address, "Error preparing query, got " + response);
                        if (metricsEnabled())
                            metrics().getErrorMetrics().getOthers().inc();
                        retry(false, null);
                        break;
                    default:
                        // Something's wrong, so we return but we let setFinalResult propagate the exception
                        RequestHandler.this.setFinalResult(connection, response);
                        break;
                }
            }

            @Override
            public void onException(Connection connection, Exception exception, long latency) {
                RequestHandler.this.onException(connection, exception, latency);
            }

            @Override
            public void onTimeout(Connection connection, long latency) {
                logError(connection.address, "Timeout waiting for response to prepare message");
                retry(false, null);
            }
        };
    }

    @Override
    public void onException(Connection connection, Exception exception, long latency) {

        Host queriedHost = current;
        try {
            if (connection instanceof PooledConnection)
                ((PooledConnection)connection).release();

            if (exception instanceof ConnectionException) {
                if (metricsEnabled())
                    metrics().getErrorMetrics().getConnectionErrors().inc();
                ConnectionException ce = (ConnectionException)exception;
                logError(ce.address, ce.getMessage());
                retry(false, null);
                return;
            }

            setFinalException(connection, exception);
        } catch (Exception e) {
            // This shouldn't happen, but if it does, we want to signal the callback, not let him hang indefinitively
            setFinalException(null, new DriverInternalError("An unexpected error happened while handling exception " + exception, e));
        } finally {
            if (queriedHost != null)
                manager.cluster.manager.reportLatency(queriedHost, latency);
        }
    }

    @Override
    public void onTimeout(Connection connection, long latency) {
        Host queriedHost = current;
        try {
            if (connection instanceof PooledConnection)
                ((PooledConnection)connection).release();

            logError(connection.address, "Timeout during read");
            retry(false, null);
        } catch (Exception e) {
            // This shouldn't happen, but if it does, we want to signal the callback, not let him hang indefinitively
            setFinalException(null, new DriverInternalError("An unexpected error happened while handling timeout", e));
        } finally {
            if (queriedHost != null)
                manager.cluster.manager.reportLatency(queriedHost, latency);
        }
    }

    interface Callback extends Connection.ResponseCallback {
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency);
        public void register(RequestHandler handler);
    }
}
