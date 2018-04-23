package com.ijunhai.emitter;

import com.ijunhai.metric.CollectEvent;

public abstract class Emitter {

    public void start() {}

    public void emit(CollectEvent metric) {
        if (metric.getValue() == 0) {
            return;
        }
        innerEmit(metric);
    }

    public abstract void innerEmit(CollectEvent metric);

    public void stop() {}
}
