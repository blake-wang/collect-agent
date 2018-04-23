/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ijunhai.rocketmq.mysql.productor;

import com.ijunhai.rocketmq.mysql.Config;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RocketMQProducer implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQProducer.class);

    private DefaultMQProducer producer;
    private Config config;

    public RocketMQProducer(Config config) throws IOException {
        this.config = config;
    }

    public void start() throws Exception {
        producer = new DefaultMQProducer("BINLOG_PRODUCER_GROUP");
        producer.setNamesrvAddr(config.getMqNamesrvAddr());
        producer.start();
    }

    public boolean push(String json) {

        SendResult sendResult = null;
        for (int i = 0; i < 3; i++) {
            try {
                Message message = new Message(config.getMqTopic(), json.getBytes("UTF-8"));
                sendResult = producer.send(message);
                if (sendResult != null && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                    return true;
                }
            } catch (Exception ex) {
                LOGGER.warn("Send message failed, retried "+(i+1)+" times", ex);
            }
        }

        return false;
    }

    @Override
    public void stop() {
        producer.shutdown();
    }
}