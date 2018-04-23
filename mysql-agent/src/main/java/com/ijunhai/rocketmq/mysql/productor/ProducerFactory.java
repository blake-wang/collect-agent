package com.ijunhai.rocketmq.mysql.productor;

import com.ijunhai.rocketmq.mysql.Config;
import com.ijunhai.storage.StorageType;

import java.io.IOException;

public class ProducerFactory {

    private Config config;

    public ProducerFactory(Config config) {
        this.config = config;
    }

    public Producer createProducer() throws IOException {
        if (StorageType.ROCKETMQ.name().toUpperCase().equals(config.getStorageType().toUpperCase())) {
            return new RocketMQProducer(config);
        } else {
            return new FileProducer(config);
        }
    }
}
