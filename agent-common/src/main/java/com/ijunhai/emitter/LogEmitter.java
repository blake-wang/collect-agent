package com.ijunhai.emitter;

import com.alibaba.fastjson.JSON;
import com.ijunhai.metric.CollectEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogEmitter extends Emitter {

    private static final Logger logger = LoggerFactory.getLogger(LogEmitter.class);

    @Override
    public void innerEmit(CollectEvent metric) {
        logger.info(JSON.toJSONString(metric));
    }
}
