package com.ijunhai;

import ch.qos.logback.core.joran.spi.JoranException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.ijunhai.serde.MessageSerde;
import com.ijunhai.serde.WrapMessage;
import com.ijunhai.util.PropertiesUtils;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.ijunhai.TaildirConstants.*;

public class ReliableTaildirEventReaderTest {

    private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReaderTest.class);

    private DefaultMQPullConsumer consumer;
    private static NamesrvController namesrvController;
    private static BrokerController brokerController;

    File tmpDir;
    File posFilePath;
    @Before
    public void setUp() {
        tmpDir = Files.createTempDir();
        posFilePath = new File(tmpDir.getAbsolutePath() + "/position_test.json");
    }

    @After
    public void tearDown() {
        try {
            FileUtils.forceDelete(tmpDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void startMQ() throws Exception {

        /*
        start nameserver
         */
        startNamesrv();

        /*
        start broker
         */
        startBroker();

        Thread.sleep(3000);
    }

    private static void startNamesrv() throws Exception {

        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);

        namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        boolean initResult = namesrvController.initialize();
        if (!initResult) {
            namesrvController.shutdown();
            throw new Exception();
        }
        namesrvController.start();
    }

    private static void startBroker() throws Exception {

        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        brokerConfig.setBrokerId(MixAll.MASTER_ID);
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        boolean initResult = brokerController.initialize();
        if (!initResult) {
            brokerController.shutdown();
            throw new Exception();
        }
        brokerController.start();
    }

    @Test
    public void testReader() throws IOException, MQClientException, InterruptedException, RemotingException, MQBrokerException, JoranException, SQLException, ClassNotFoundException {
        PropertiesUtils.init();
//        String fileLastModifyStart = PropertiesUtils.get(FILE_LASTMODIFY_START);
//        long fileLastModifyStamp = LocalDateTime.parse(fileLastModifyStart, DateTimeFormatter.ofPattern("yyyyMMddHHmm"))
//                .atZone(ZoneId.of("+08:00")).toEpochSecond();
//        long fileLastModifyMillis = fileLastModifyStamp * 1000;
//        final ReliableTaildirEventReader taildirReader = new ReliableTaildirEventReader(
//                new File(PropertiesUtils.get(POSITION_FILE)),
//                fileLastModifyMillis
//        );
//        List<Future> futures = taildirReader.start();
//        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
//            @Override
//            public void run() {
//                taildirReader.stop();
//            }
//        }));
//
//        futures.forEach(future -> {
//            try {
//                future.get();
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        });
//        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
//        producer.setNamesrvAddr(nameServer);
//        producer.setRetryTimesWhenSendFailed(1);
//        producer.start();
//        for (int i = 0; i < 100; i++) {
//            byte[] bytes = ("a"+i).getBytes();
//            producer.send(new Message(TOPIC_DEFAULT, bytes));
//        }


                /*
        consumer message
         */
//        consumer = new DefaultMQPullConsumer("aaa");
//        consumer.setNamesrvAddr(PropertiesUtils.get(ROCKETMQ_NAMESERVER));
//        consumer.setMessageModel(MessageModel.valueOf("BROADCASTING"));
//        consumer.registerMessageQueueListener("YouyunOrder", null);
//        consumer.start();
//
//        int i = 0;
//        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues("YouyunOrder");
//        for (MessageQueue queue : queues) {
//            long startOffset = getMessageQueueOffset(queue);
//            long lastoffset = consumer.maxOffset(queue);
//            while (lastoffset > startOffset) {
//                PullResult pullResult = consumer.pull(queue, null, startOffset, 32);
//
//                if (pullResult.getPullStatus() == PullStatus.FOUND) {
//                    for (MessageExt message : pullResult.getMsgFoundList()) {
//                        startOffset = message.getQueueOffset()+1;
//                        WrapMessage wrapMessage = MessageSerde.deserialize(message);
////                        String[] lines = new String(wrapMessage.getMessageBody()).split("\n");
//                        if ("/data2/IMPORTANT_DATA/collect_data/youyun_order_2017-10-28".equals(wrapMessage.getFilePath())) {
//                            String[] lines = new String(wrapMessage.getMessageBody()).split("\n");
//                            i+=lines.length;
//                            Arrays.stream(lines).forEach(line-> {
//                                JSONObject jsonObject = JSON.parseObject(line);
//                                JSONArray rows = jsonObject.getJSONArray("rows");
//                                for (int idx = 0; idx < rows.size(); idx++) {
//                                    JSONObject rowsJSONObject = rows.getJSONObject(idx).getJSONObject("data");
//                                    if ("2017102831830180059".equals(rowsJSONObject.getString("order_sn"))) {
//                                        System.out.println(rowsJSONObject.toJSONString());
//                                    }
//                                }
////                                Integer xid = (Integer)jsonObject.get("xid");
////                                if (2520975 ==xid) {
////                                    System.out.println(wrapMessage.getLineCount());
////                                }
//
//                            });
//
//
////                            for(byte b : wrapMessage.getMessageBody()) {
////                                if (b=='\n') {
////                                    i++;
////                                }
////                            }
////                            fileWriter.write(new String(wrapMessage.getMessageBody()));
//                        }
////                    i++;
////                    System.out.println(i+"receive message : " + wrapMessage.getLineCount());
////                    System.out.println(new String(wrapMessage.getMessageBody()));
//                    }
//
////                long nextBeginOffset = pullResult.getNextBeginOffset();
////                putMessageQueueOffset(queue, nextBeginOffset);
//                }
//            }
//
//        }
//        System.out.println("========================"+i);
//        /*
//        wait for processQueueTable init
//         */
//        Thread.sleep(3000);
//
//        consumer.shutdown();

    }

    private long getMessageQueueOffset(MessageQueue queue) throws MQClientException {

        long offset = consumer.fetchConsumeOffset(queue, false);
        if (offset < 0) {
            offset = 0;
        }

        return offset;
    }
    private void putMessageQueueOffset(MessageQueue queue, long offset) throws MQClientException {
        consumer.updateConsumeOffset(queue, offset);
    }

    @AfterClass
    public static void stop() {

        if (brokerController != null) {
            brokerController.shutdown();
        }

        if (namesrvController != null) {
            namesrvController.shutdown();
        }
    }
}

