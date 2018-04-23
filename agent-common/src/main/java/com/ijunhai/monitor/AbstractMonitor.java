package com.ijunhai.monitor;

import com.ijunhai.emitter.Emitter;

public abstract class AbstractMonitor implements Monitor {
    private volatile boolean started = false;

    public void start() {
        this.started = true;
    }

    public void stop() {
        this.started = false;
    }

    public boolean monitor(Emitter emitter) {
        return this.started ? this.doMonitor(emitter) : false;
    }

    public abstract boolean doMonitor(Emitter emitter);


}

