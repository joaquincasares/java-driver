package com.datastax.driver.core.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritePipeline extends Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(WritePipeline.class);

    WritePipeline(Builder builder) {
        super(builder);
    }

    /**
     * Blocks until all pending statements and in-flight requests have
     * returned.
     */
    @Override
    public void confirm() {
        // this Event() is set in maximizeInFlightRequests()'s callback if all sent requests have
        // returned and cleared each time execute() is called since another
        // statement is added to the `Pipeline.statements` Queue
        try {
            completedRequests.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
