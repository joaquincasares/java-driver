package com.datastax.driver.core.pipeline;

import com.datastax.driver.core.ResultSetFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * The ``ReadPipeline`` is a helper object meant to add high-performance
 * read request pipelining to any application with minimal code changes.
 *
 * A `Session` object is first created and used
 * to initiate the ``ReadPipeline``. The ``ReadPipeline`` can then be
 * used to execute multiple queries asynchronously using
 * `Pipeline.execute()`.
 *
 * As `Pipeline.maxInFlightRequests` is reached,
 * statements are queued up for execution as soon as a
 * `ResponseSetFuture` is returned.
 *
 * Once all requests have been sent into the ``ReadPipeline`` and the
 * business logic requires consuming those read requests,
 * `ReadPipeline.results()` will return an iterator
 * of `ResponseSetFuture` objects.
 *
 * The result from `ReadPipeline.results().get()` is an
 * iterator of `ResultSet` objects which in turn
 * another iterator over the result's rows and the same type of object
 * returned when calling `Session.execute()`.
 *
 * The results from `ReadPipeline.results()` follow
 * the same ordering as statements that went into
 * `Pipeline.execute()` without any top-level
 * indication of the keyspace, table, nor query that was called that is not
 * already accessible within the `ResultSet`.
 *
 * It's recommended to keep a dedicated readPipeline for each query unless
 * schemas are identical or business logic will handle the different types of
 * returned Cassandra rows.
 *
 * `Pipeline.execute()` passes the `Statement`
 * `Session.execute_async()`
 * internally to increase familiarity with standard java-driver usage.
 * By default, `PreparedStatement` queries will be
 * processed as expected.
 *
 * If `SimpleStatement`, `BoundStatement`,
 * and `BatchStatement` statements need to be processed,
 * `Pipeline.allowNonPerformantQueries` will
 * need to be set to `True`. `BatchStatements`
 * should only be used if all statements will modify the same partition to
 * avoid Cassandra anti-patterns.
 *
 * Example usage::
 *
 * >>> from cassandra.cluster import Cluster
 * >>> from cassandra.concurrent import ReadPipeline
 * >>> cluster = Cluster(['192.168.1.1', '192.168.1.2'])
 * >>> session = cluster.connect()
 * >>> read_pipeline = ReadPipeline(session)
 * >>> prepared_statement = session.prepare('SELECT * FROM mykeyspace.users WHERE name = ?')
 * >>> read_pipeline.execute(prepared_statement, ('Jorge'))
 * >>> read_pipeline.execute(prepared_statement, ('Jose'))
 * >>> read_pipeline.execute(prepared_statement, ('Sara'))
 * >>> prepared_statement = session.prepare('SELECT * FROM old_keyspace.old_users WHERE name = ?')
 * >>> read_pipeline.execute(prepared_statement, ('Jorge'))
 * >>> read_pipeline.execute(prepared_statement, ('Jose'))
 * >>> read_pipeline.execute(prepared_statement, ('Sara'))
 * >>> for result in read_pipeline.results():
 * ...     for row in result:
 * ...         print row.name, row.age
 * >>> ...
 * >>> cluster.shutdown()
 */
public class ReadPipeline extends Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(ReadPipeline.class);

    ReadPipeline(Builder builder) {
        super(builder);
    }

    /**
     * This method is only for the WritePipeline since it only ensures
     * the requests were processed correctly. For ReadPipelines we will
     * want to consume those requests.
     */
    @Override
    void confirm() {
        throw new UnsupportedOperationException();
    }

    /**
     * Iterate over and return all read request `future.results()`.
     *
     * @return An iterator of Cassandra `ResultSetFuture` objects, which in
     * turn are iterators over ResultSet rows from a query result.
     */
    Iterator<ResultSetFuture> results() {
        return new ResultIterator(this);
    }

    class ResultIterator implements Iterator {
        ReadPipeline pipeline;

        /**
         * Iterates over a `ReadPipeline` object to consume all results
         * in the order they were received.
         *
         * @param readPipeline
         */
        public ResultIterator(ReadPipeline readPipeline) {
            pipeline = readPipeline;
        }

        /**
         * Checks the ReadPipeline to see if all pending futures and statements
         * have been processed.
         *
         * @return `True` when there are still more results to consume.
         * `False` when there are no more results to consume.
         */
        @Override
        public boolean hasNext() {
            return pipeline.completedRequests.availablePermits() < 0;
        }

        /**
         * Iterates over unconsumed Cassandra read requests and delivers
         * ResultSetFuture objects which should be consumed via `future.get()`.
         *
         * @return In order Cassandra read request futures.
         */
        @Override
        public ResultSetFuture next() {
            ResultSetFuture future = pipeline.futures.remove();

            // always ensure that at least one future has been queued.
            // useful for cases where `maxUnconsumedReadResponses == 0`
            pipeline.maximizeInFlightRequests();

            // yield the ResultSetFuture which in turn is another iterator over
            // the rows within the query's result
            return future;
        }

        /**
         * The `remove()` method is not supported since all Cassandra
         * futures are meant for application consumption.
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
