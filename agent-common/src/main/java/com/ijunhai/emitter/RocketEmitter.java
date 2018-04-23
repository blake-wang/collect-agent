package com.ijunhai.emitter;

import com.alibaba.fastjson.JSON;
import com.ijunhai.metric.CollectEvent;
import com.ijunhai.util.PropertiesUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ijunhai.emitter.RocketEmitterConstants.ROCKETMQ_EMITTER_NAMESERVER;
import static com.ijunhai.emitter.RocketEmitterConstants.ROCKETMQ_EMITTER_PRODUCERGROUP;
import static com.ijunhai.emitter.RocketEmitterConstants.ROCKETMQ_EMITTER_TOPIC;

public class RocketEmitter extends Emitter {

    private static final Logger logger = LoggerFactory.getLogger(RocketEmitter.class);

    private DefaultMQProducer producer;
    private String topic;

    @Override
    public void start() {
        this.topic = PropertiesUtils.get(ROCKETMQ_EMITTER_TOPIC);
        producer = new DefaultMQProducer(PropertiesUtils.get(ROCKETMQ_EMITTER_PRODUCERGROUP));
        producer.setNamesrvAddr(PropertiesUtils.get(ROCKETMQ_EMITTER_NAMESERVER));
        producer.setRetryTimesWhenSendFailed(1);
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("Cannot start RocketEmitter");
        }

    }

    @Override
    public void stop() {
        producer.shutdown();
    }

    @Override
    public void innerEmit(CollectEvent metric) {

        try {
            producer.send(
                    new Message(
                            topic,
                            topic,
                            JSON.toJSONBytes(metric)
                    ),
                    1000
            );
        } catch (Exception e) {
            logger.warn("Send metric failed, " + e.getMessage());
        }
    }
}
