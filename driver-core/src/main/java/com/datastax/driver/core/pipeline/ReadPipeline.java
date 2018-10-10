package com.datastax.driver.core.pipeline;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WritePipeline extends Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private WritePipeline(Builder builder) {
        super(builder);
    }

    @Override
    public void confirm() {
        try {
            completedRequests.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public class Builder {
        // the Cassandra session object
        protected Session session;

        // set max_in_flight_requests
        protected int maxInFlightRequests = 100;

        // set the maximum number of unsent write requests before the Pipeline
        // blocks to ensure that all in-flight write requests have been
        // processed and confirmed to not have thrown any exceptions.
        // ignore the maximum size of pending statements if set to None/0/False
        protected int maxUnsentWriteRequests = 40000;

        // set the maximum number of unconsumed futures to hold onto
        // before continuing to process more pending read requests
        protected int maxUnconsumedReadResponses = 0;

        // use a custom errorHandler function upon future.result() errors
        protected Class errorHandler = null;

        // allow for Statements, BoundStatements, and BatchStatements to be
        // processed. By default, only PreparedStatements are processed.
        protected boolean allowNonPerformantQueries = false;

        public Builder(Session session) {
            this.session = session;
        }

        public Builder maxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder maxUnsentWriteRequests(int maxUnsentWriteRequests) {
            this.maxUnsentWriteRequests = maxUnsentWriteRequests;
            return this;
        }

        public Builder maxUnconsumedReadResponses(int maxUnconsumedReadResponses) {
            this.maxUnconsumedReadResponses = maxUnconsumedReadResponses;
            return this;
        }

        public Builder errorHandler(Class errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        public Builder allowNonPerformantQueries(boolean allowNonPerformantQueries) {
            this.allowNonPerformantQueries = allowNonPerformantQueries;
            return this;
        }

        public WritePipeline build() {
            // ensure that we are not using settings for the ReadPipeline within
            // the WritePipeline, or vice versa
            if (this.maxUnsentWriteRequests > 0 && this.maxUnconsumedReadResponses > 0) {
                throw new IllegalArgumentException("The pipeline can either be a Read or Write Pipeline, not both." +
                        " As such, maxUnsentWriteRequests and maxUnconsumedReadResponses cannot both be non-zero.");
            }
            return new WritePipeline(this);
        }
    }
}
