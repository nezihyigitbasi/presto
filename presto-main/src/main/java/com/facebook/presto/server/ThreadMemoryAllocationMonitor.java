package com.facebook.presto.server;

import com.sun.management.ThreadMXBean;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

final class ThreadMemoryAllocationMonitor
{
    private static final Logger log = Logger.get(ThreadMemoryAllocationMonitor.class);
    private static final AtomicBoolean installed = new AtomicBoolean();

    private final Duration interval;
    private final DataSize threshold;
    private final ThreadMXBean threadMXBean;
    private final boolean isMonitoringEnabled;

    @Inject
    public ThreadMemoryAllocationMonitor()
    {
        //TODO from configuration
        interval = new Duration(10, SECONDS);
        threshold = new DataSize(100, MEGABYTE);
        threadMXBean = ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean());
        isMonitoringEnabled = threadMXBean.isThreadAllocatedMemorySupported() && threadMXBean.isThreadAllocatedMemoryEnabled();
    }

    @PostConstruct
    public void start()
    {
        if (isMonitoringEnabled) {
            log.info("Starting the allocation monitoring thread ...");
            startMonitoringAllocations();
        }
    }

    public void startMonitoringAllocations()
    {
        if (installed.getAndSet(true)) {
            return;
        }

        Thread monitoringThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                long[] threads = threadMXBean.getAllThreadIds();
                long[] allocations = threadMXBean.getThreadAllocatedBytes(threads);
                verify(threads.length == allocations.length, "Threads and allocations must have the same length");

                for (int i = 0; i < threads.length; i++) {
                    long threadId = threads[i];
                    long allocation = allocations[i];

                    if (allocation >= threshold.toBytes()) {
                        log.info("Thread %s has allocated %s so far", threadMXBean.getThreadInfo(threadId).getThreadName(), DataSize.succinctBytes(allocation));
                    }
                }

                try {
                    MILLISECONDS.sleep(interval.toMillis());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        monitoringThread.setDaemon(true);
        monitoringThread.setName("Presto-Allocation-Monitoring-Thread");
        monitoringThread.start();
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        ThreadMemoryAllocationMonitor t = new ThreadMemoryAllocationMonitor();
        t.start();

        while(true)
        {
            byte[] b = new byte[200 * 1024 * 1024];
            for (int i = 0 ; i < b.length; i++) {
                b[i] = 0x0f;
            }
            Thread.sleep(1000);
        }
    }
}
