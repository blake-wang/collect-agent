package com.ijunhai.metric;

import java.util.concurrent.atomic.AtomicLong;

public class CollectMetrics {

    private final AtomicLong logReadedCount = new AtomicLong(0);
    private final AtomicLong sendedCount = new AtomicLong(0);

    public long logReaded() {
        return logReadedCount.get();
    }

    public long sended() {
        return sendedCount.get();
    }

    public void incrementLogReaded() {
        logReadedCount.incrementAndGet();
    }

    public void incrementLogReaded(long count) {
        logReadedCount.addAndGet(count);
    }

    public void incrementSended() {
        sendedCount.incrementAndGet();
    }

    public void incrementSended(long count) {
        sendedCount.addAndGet(count);
    }

    public CollectMetrics snapshot() {
        CollectMetrics snapshot = new CollectMetrics();
        snapshot.logReadedCount.set(logReadedCount.get());
        snapshot.sendedCount.set(sendedCount.get());
        return snapshot;
    }
}
