package com.ijunhai.monitor;

import com.ijunhai.emitter.Emitter;

public interface Monitor {

    void start();

    void stop();

    boolean monitor(Emitter emitter);
}
