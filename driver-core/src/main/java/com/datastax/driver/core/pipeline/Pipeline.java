package com.datastax.driver.core.pipeline;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This ``Pipeline`` object is not meant to be used by itself and instead
 * should be used via the `pipeline.WritePipeline` or
 * `pipeline.ReadPipeline` objects.
 *
 * This ``Pipeline`` object is missing fundamental functionality to ensure
 * write requests are processed or read requests are able to be read.
 */
abstract public class Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    // the Cassandra session object
    private final Session session;

    // set max_in_flight_requests
    private final int maxInFlightRequests;

    // set the maximum number of unsent write requests before the Pipeline
    // blocks to ensure that all in-flight write requests have been
    // processed and confirmed to not have thrown any exceptions.
    // ignore the maximum size of pending statements if set to None/0/False
    private final int maxUnsentWriteRequests;

    // set the maximum number of unconsumed futures to hold onto
    // before continuing to process more pending read requests
    private final int maxUnconsumedReadResponses;

    // use a custom errorHandler function upon future.result() errors
    private final PipelineErrorHandler errorHandler;

    // allow for Statements, BoundStatements, and BatchStatements to be
    // processed. By default, only PreparedStatements are processed.
    private final boolean allowNonPerformantQueries;

    // hold futures for the ReadPipeline superclass
    final Queue<ResultSetFuture> futures = new LinkedList<ResultSetFuture>();

    // store all pending PreparedStatements along with matching args/kwargs
    private final Queue<Statement> statements = new LinkedList<Statement>();

    // track when all pending statements and futures have returned
    final Semaphore completedRequests = new Semaphore(1);

    // track the number of in-flight futures and completed statements
    // always to be used with an in_flight_counter_lock
    private int inFlightCounter = 0;

    // ensure that this.completed_requests will never be set() between:
    // 1. emptying the this.statements
    // 2. creating the last future
    private final Lock inFlightCounterLock = new ReentrantLock();

    Pipeline(Builder builder) {
        session = builder.session;
        maxInFlightRequests = builder.maxInFlightRequests;
        maxUnsentWriteRequests = builder.maxUnsentWriteRequests;
        maxUnconsumedReadResponses = builder.maxUnconsumedReadResponses;
        errorHandler = builder.errorHandler;
        allowNonPerformantQueries = builder.allowNonPerformantQueries;
    }

    /**
     * This code is called multiple times within the Pipeline infrastructure
     * to ensure we're keeping as many in-flight requests processing as
     * possible.
     *
     * We're cautious here and default to a no-op in cases where:
     *
     * * There are already too many in-flight requests.
     * * We already have too many `ResultSetFuture`
     *   objects taking up memory.
     * * There are no more statements that need to be processed.
     *
     * In all other cases, we call
     * `Session.execute_async()` with a queued
     * Statement, and add a callback to the future.
     *
     * In cases where we're using a `ReadPipeline`, we will store the future
     * for later consumption of the `ResultSetFuture`
     * objects by way of `ReadPipeline.results()`.
     */
    void maximizeInFlightRequests() {
        // convert pending statements to in-flight futures if we haven't hit our
        // threshold
        if (this.inFlightCounter > this.maxInFlightRequests) {
            return;
        }

        // convert pending statements to in-flight futures if there aren't too
        // many futures that have not been processed.
        // if there are too many futures, wait until ReadPipeline.results()
        // has been called to start consuming futures and processing new
        // statements in parallel with potentially costly business logic
        if (this.maxUnconsumedReadResponses > 0 && this.futures.size() > this.maxUnconsumedReadResponses) {
            return;
        }

        // grab the next statement, if still available
        Statement statement;
        inFlightCounterLock.lock();
        try {
            // keep track of the number of in-flight requests
            statement = this.statements.remove();
            ++this.inFlightCounter;
        } catch (NoSuchElementException emptyStatementsQueue) {
            // exit early if there are no more statements to process
            return;
        } finally {
            inFlightCounterLock.unlock();
        }
        // keep track of the number of in-flight requests
        this.completedRequests.tryAcquire();

        // send the statement to Cassandra
        ResultSetFuture future = this.session.executeAsync(statement);

        // if we're processing read requests,
        // hold onto the future for later processing
        if (this.maxUnconsumedReadResponses > 0) {
            this.futures.add(future);
        } else {
            // if we're processing write requests,
            // await for the future's callback
            Futures.addCallback(future, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet rows) {
                    inFlightCounterLock.lock();
                    try {
                        // keep track of the number of in-flight requests
                        --inFlightCounter;
                        if (inFlightCounter < 1) {
                            if (statements.isEmpty()) {
                                if (inFlightCounter < 0) {
                                    throw new RuntimeException("The in_flight_counter should never have been less" +
                                            " than 0. The lock mechanism is not working as expected!");
                                }
                            }
                        }
                        maximizeInFlightRequests();
                    } finally {
                        inFlightCounterLock.unlock();
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    errorHandler.handle(throwable);
                }
            });
        }
    }

    public void execute(Statement statement) {
        // to ensure maximum throughput, only interact with PreparedStatements
        // as is the best practice
        if (!this.allowNonPerformantQueries && !(statement instanceof PreparedStatement)) {
            throw new IllegalArgumentException("Only PreparedStatements are allowed when using the Pipeline." +
                    " If other Statement types must be used, set Pipeline.this.allowNonPerformantQueries to `True`" +
                    " with the understanding that there may some performance hit since SimpleStatements will require" +
                    " server-side processing and BatchStatements should only contain mutations targeting the same" +
                    " partition to avoid a Cassandra anti-pattern.");
        }

        // if the soft maximum size of pending statements has been exceeded,
        // wait until all pending statements and in-flight futures have returned
        // ignore the maximum size of pending statements if set to None/0/False
        if (this.maxUnsentWriteRequests > 0 && this.statements.size() > this.maxUnsentWriteRequests) {
            this.confirm();
        }

        // reset the this.completed_requests Event and block on this.confirm()
        // until the new statement has been processed
        this.completedRequests.tryAcquire();

        // add the new statement to the pending statements Queue
        this.statements.add(statement);

        // attempt to process the newest statement
        this.maximizeInFlightRequests();
    }

    abstract void confirm();

    public class Builder {
        // the Cassandra session object
        private Session session;

        // set max_in_flight_requests
        private int maxInFlightRequests = 100;

        // set the maximum number of unsent write requests before the Pipeline
        // blocks to ensure that all in-flight write requests have been
        // processed and confirmed to not have thrown any exceptions.
        // ignore the maximum size of pending statements if set to None/0/False
        private int maxUnsentWriteRequests = 40000;

        // set the maximum number of unconsumed futures to hold onto
        // before continuing to process more pending read requests
        private int maxUnconsumedReadResponses = 0;

        // use a custom errorHandler function upon future.result() errors
        private PipelineErrorHandler errorHandler = null;

        // allow for Statements, BoundStatements, and BatchStatements to be
        // processed. By default, only PreparedStatements are processed.
        private boolean allowNonPerformantQueries = false;

        public Builder(Session session) {
            this.session = session;
        }

        public Builder maxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder maxUnsentWriteRequests(int maxUnsentWriteRequests) {
            // ensure that we are not using settings for the ReadPipeline within
            // the WritePipeline, or vice versa
            if (this.maxUnsentWriteRequests > 0 && this.maxUnconsumedReadResponses > 0) {
                throw new IllegalArgumentException("The pipeline can either be a Read or Write Pipeline, not both." +
                        " As such, maxUnsentWriteRequests and maxUnconsumedReadResponses cannot both be non-zero.");
            }
            this.maxUnsentWriteRequests = maxUnsentWriteRequests;
            return this;
        }

        public Builder maxUnconsumedReadResponses(int maxUnconsumedReadResponses) {
            // ensure that we are not using settings for the ReadPipeline within
            // the WritePipeline, or vice versa
            if (this.maxUnsentWriteRequests > 0 && this.maxUnconsumedReadResponses > 0) {
                throw new IllegalArgumentException("The pipeline can either be a Read or Write Pipeline, not both." +
                        " As such, maxUnsentWriteRequests and maxUnconsumedReadResponses cannot both be non-zero.");
            }
            this.maxUnconsumedReadResponses = maxUnconsumedReadResponses;
            return this;
        }

        public Builder errorHandler(PipelineErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        public Builder allowNonPerformantQueries(boolean allowNonPerformantQueries) {
            this.allowNonPerformantQueries = allowNonPerformantQueries;
            return this;
        }

        public WritePipeline buildWritePipeline() {
            return new WritePipeline(this);
        }

        public ReadPipeline buildReadPipeline() {
            return new ReadPipeline(this);
        }
    }
}

