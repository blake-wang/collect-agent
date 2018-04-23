package com.ijunhai.monitor;

import com.google.common.collect.Sets;
import com.ijunhai.emitter.Emitter;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MonitorScheduler {

    private final ScheduledExecutorService exec;
    private final Emitter emitter;
    private final Set<Monitor> monitors;
    private final Object lock = new Object();

    private volatile boolean started = false;

    public MonitorScheduler(
            ScheduledExecutorService exec,
            Emitter emitter
    ) {
        this.exec = exec;
        this.emitter = emitter;
        this.monitors = Sets.newHashSet();
    }

    public void start() {
        synchronized (lock) {
            if (started) {
                return;
            }
            started = true;
            startMonitor();
        }
        emitter.start();
    }

    public void addMonitor(final Monitor monitor) {
        synchronized (lock) {
            monitors.add(monitor);
            monitor.start();
        }
    }


    public void stop() {
        synchronized (lock) {
            if (!started) {
                return;
            }

            started = false;
            for (Monitor monitor : monitors) {
                monitor.monitor(emitter);
                monitor.stop();
            }
        }
        emitter.stop();
    }

    private void startMonitor() {
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                monitors.forEach(monitor -> monitor.monitor(emitter));
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

}
