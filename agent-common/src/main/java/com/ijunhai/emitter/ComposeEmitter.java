package com.ijunhai.emitter;

import com.google.common.annotations.VisibleForTesting;
import com.ijunhai.metric.CollectEvent;
import com.ijunhai.metric.EmitterType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ComposeEmitter extends Emitter {

    private static final Logger logger = LoggerFactory.getLogger(ComposeEmitter.class);

    private List<Emitter> emitters = new ArrayList<>();

    public ComposeEmitter(String emittersStr) {
        if (StringUtils.isNotBlank(emittersStr)) {
            String[] split = emittersStr.split(",");
            for (String emitterStr : split) {
                EmitterType emitterType = EmitterType.valueOf(emitterStr.trim().toUpperCase());
                if (emitterType == null) {
                    continue;
                }
                logger.info("add emitter: " + emitterType.name());
                switch (emitterType) {
                    case ROCKETMQ:
                        emitters.add(new RocketEmitter());
                        break;
                    case LOGGING:
                        emitters.add(new LogEmitter());
                        break;
                }
            }
        }
        if (emitters.size() == 0) {
            logger.info("add emitter: " + EmitterType.LOGGING.name());
            emitters.add(new LogEmitter());
        }
    }

    @Override
    public void start() {
        for (Emitter emitter : emitters) {
            emitter.start();
        }
    }

    @Override
    public void stop() {
        for (Emitter emitter : emitters) {
            emitter.stop();
        }
    }

    @Override
    public void innerEmit(CollectEvent metric) {
        for (Emitter emitter : emitters) {
            emitter.emit(metric);
        }
    }

    @VisibleForTesting
    public List<Emitter> getEmitters() {
        return emitters;
    }
}
