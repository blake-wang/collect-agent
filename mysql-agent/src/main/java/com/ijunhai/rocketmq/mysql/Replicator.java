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

package com.ijunhai.rocketmq.mysql;

import ch.qos.logback.core.joran.spi.JoranException;
import com.ijunhai.emitter.ComposeEmitter;
import com.ijunhai.emitter.Emitter;
import com.ijunhai.emitter.LogEmitter;
import com.ijunhai.emitter.RocketEmitter;
import com.ijunhai.metric.CollectMetrics;
import com.ijunhai.metric.EmitterType;
import com.ijunhai.metric.MetricType;
import com.ijunhai.monitor.CollectMonitor;
import com.ijunhai.monitor.MonitorScheduler;
import com.ijunhai.rocketmq.mysql.binlog.EventProcessor;
import com.ijunhai.rocketmq.mysql.binlog.Transaction;
import com.ijunhai.rocketmq.mysql.position.BinlogPosition;
import com.ijunhai.rocketmq.mysql.position.BinlogPositionLogThread;
import com.ijunhai.rocketmq.mysql.productor.Producer;
import com.ijunhai.rocketmq.mysql.productor.ProducerFactory;
import com.ijunhai.rocketmq.mysql.productor.RocketMQProducer;
import com.ijunhai.util.CommandLineUtils;
import com.ijunhai.util.LogBackConfigLoader;
import com.ijunhai.util.PropertiesUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;

import static com.ijunhai.metric.CollectEvent.HOST;

public class Replicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    private static final Logger POSITION_LOGGER = LoggerFactory.getLogger("PositionLogger");

    private final String host = PropertiesUtils.get(HOST);

    private static Config config;

    private EventProcessor eventProcessor;

    private Producer producer;

    private Object lock = new Object();
    private BinlogPosition nextBinlogPosition;
    private long xid;

    private CollectMonitor monitor;

    public static void main(String[] args) throws IOException, ParseException, JoranException {
        CommandLine commandLine = CommandLineUtils.genCommandLine(args);
        if (commandLine.hasOption('l')) {
            String filePath = commandLine.getOptionValue('l');
            LOGGER.info("logback file [{}]", filePath);
            LogBackConfigLoader.load(filePath);
        }
        config = new Config();
        if (commandLine.hasOption('c')) {
            String filePath = commandLine.getOptionValue('c');
            LOGGER.info("config file [{}]", filePath);
            config.load(commandLine.getOptionValue('c'));
        } else {
            config.load();
        }

        Replicator replicator = new Replicator();
        replicator.start();
    }

    public void start() {
        try {
            final Emitter emitter = new ComposeEmitter(config.getEmitter());

            monitor = new CollectMonitor(host, config.getMqTopic(), "mysql", MetricType.Mysql, new CollectMetrics());
            MonitorScheduler monitorScheduler = new MonitorScheduler(Executors.newSingleThreadScheduledExecutor(), emitter);
            monitorScheduler.addMonitor(monitor);
            monitorScheduler.start();

            producer = new ProducerFactory(config).createProducer();
            producer.start();

            BinlogPositionLogThread binlogPositionLogThread = new BinlogPositionLogThread(this);
            binlogPositionLogThread.start();

            eventProcessor = new EventProcessor(this);
            eventProcessor.start();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    producer.stop();
                    eventProcessor.stopOpenReplicator();
                }
            }));

        } catch (Exception e) {
            LOGGER.error("Start error.", e);
            System.exit(1);
        }
    }



    public void commit(Transaction transaction, boolean isCompleted) {
        try {
            if (transaction.getRowsCount() < 1) {
                return;
            }
            String json = transaction.toJson();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send message [{}]", json);
            }

            if (isCompleted) {
                synchronized (lock) {
                    xid = transaction.getXid();
                    nextBinlogPosition = transaction.getNextBinlogPosition();
                }
            }
            if (!producer.push(json)) {
                LOGGER.error("send message failed, will reconnect mysql");
                eventProcessor.stopOpenReplicator();
            } else {
                monitor.getCollectMetrics().incrementSended(transaction.getRowsCount());
            }

        } catch (Exception ignored) {

        }
    }

    public void logPosition() {

        String binlogFilename = null;
        long xid = 0L;
        long nextPosition = 0L;

        synchronized (lock) {
            if (nextBinlogPosition != null) {
                xid = this.xid;
                binlogFilename = nextBinlogPosition.getBinlogFilename();
                nextPosition = nextBinlogPosition.getPosition();
            }
        }

        if (binlogFilename != null) {
            POSITION_LOGGER.info("XID: {},   BINLOG_FILE: {},   NEXT_POSITION: {}",
                    xid, binlogFilename, nextPosition);
        }

    }

    public Config getConfig() {
        return config;
    }

    public CollectMonitor getMonitor() {
        return monitor;
    }


    public BinlogPosition getNextBinlogPosition() {
        return nextBinlogPosition;
    }

}
