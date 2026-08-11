/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.exchange;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpoolingExchangeDataSource
{
    private static final Slice DATA = Slices.utf8Slice("data");
    private static final long MEMORY_USAGE = 42;

    @Test
    public void testMemoryUsageTracking()
    {
        AggregatedMemoryContext operatorMemoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
        LocalMemoryContext memoryContext = operatorMemoryContext.newLocalMemoryContext("test");
        SpoolingExchangeDataSource dataSource = new SpoolingExchangeDataSource(new TestingExchangeSource(), memoryContext);

        assertThat(dataSource.pollPage()).isEqualTo(DATA);
        assertThat(memoryContext.getBytes()).isEqualTo(MEMORY_USAGE);

        dataSource.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    /**
     * The data source is shared by all the drivers of a pipeline, so one driver can close it and destroy the owning
     * operator context while another driver is reading. Polling must not fail in that case.
     */
    @Test
    public void testPollPageConcurrentWithCloseAndOperatorContextDestroy()
            throws Exception
    {
        AggregatedMemoryContext operatorMemoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
        LocalMemoryContext memoryContext = operatorMemoryContext.newLocalMemoryContext("test");
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch closeFinished = new CountDownLatch(1);
        SpoolingExchangeDataSource dataSource = new SpoolingExchangeDataSource(
                new TestingExchangeSource(readStarted, closeFinished),
                memoryContext);

        ExecutorService executor = newSingleThreadExecutor(daemonThreadsNamed("test-poll-page"));
        try {
            Future<Slice> polledPage = executor.submit(dataSource::pollPage);
            assertThat(readStarted.await(30, SECONDS)).isTrue();

            dataSource.close();
            // mirrors OperatorContext.destroy(), which closes the aggregated memory context of the operator
            operatorMemoryContext.close();
            closeFinished.countDown();

            assertThat(polledPage.get(30, SECONDS)).isEqualTo(DATA);
            assertThat(memoryContext.getBytes()).isEqualTo(0);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static class TestingExchangeSource
            implements ExchangeSource
    {
        private final CountDownLatch readStarted;
        private final CountDownLatch readBlocked;

        public TestingExchangeSource()
        {
            this(new CountDownLatch(1), new CountDownLatch(0));
        }

        public TestingExchangeSource(CountDownLatch readStarted, CountDownLatch readBlocked)
        {
            this.readStarted = readStarted;
            this.readBlocked = readBlocked;
        }

        @Override
        public Slice read()
        {
            readStarted.countDown();
            awaitUninterruptibly(readBlocked);
            return DATA;
        }

        @Override
        public long getMemoryUsage()
        {
            return MEMORY_USAGE;
        }

        @Override
        public void addSourceHandles(List<ExchangeSourceHandle> handles)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreSourceHandles()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputSelector(ExchangeSourceOutputSelector selector)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public void close() {}
    }
}
