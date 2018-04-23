package com.ijunhai.rocketmq.mysql.position;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ijunhai.rocketmq.mysql.Config;
import com.ijunhai.serde.MessageSerde;
import com.ijunhai.serde.WrapMessage;
import com.ijunhai.storage.StorageType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Set;

public class BinlogPositionStorage {

    private static final Logger logger = LoggerFactory.getLogger(BinlogPositionStorage.class);

    private String binlogFilename;
    private Long nextPosition = 0L;
    private Config config;

    public BinlogPositionStorage(Config config) {
        this.config = config;
    }

    public Pair<String, Long> getLastBinglogFilePosition() throws Exception {
        if (StorageType.FILE.equals(StorageType.valueOf(config.getStorageType().toUpperCase()))) {
            logger.info("init position from file storage");
            initLastDataFromFile();
        } else {
            logger.info("init position from rocketmq storage");
            initLastDataFromRocketMQ();
        }
        //TODO to remove
        if (StringUtils.isBlank(binlogFilename)) {
            logger.info("init position from rocketmq storage again");
            initLastDataFromRocketMQ();
        }
        return ImmutablePair.of(binlogFilename, nextPosition);
    }

    private void initLastDataFromFile() throws IOException {
        File dataTempDir = new File(config.getDataTempPath());
        File lastFile = null;
        if (!dataTempDir.exists()) {
            logger.info("{} is not exist", config.getDataTempPath());
            return;
        }
        if (dataTempDir.listFiles().length == 0) {
            logger.info("no files in {}", config.getDataTempPath());
            return;
        }
        for (File file : dataTempDir.listFiles()) {
            if (!file.getName().startsWith(config.getFileNamePrefix())) {
                continue;
            }
            if (lastFile == null) {
                lastFile = file;
            } else {
                if (lastFile.lastModified() < file.lastModified()) {
                    lastFile = file;
                }
            }
        }
        RandomAccessFile fileReader = null;
        try {

            logger.info("find position from [{}]", lastFile);
            fileReader = new RandomAccessFile(lastFile, "r");
            long position = fileReader.length() - 1;
            if (position <=1) {
                logger.info("cannot find position from [{}], the file length is: {}" + fileReader.length(), lastFile);
            }
            while (position > 0) {
                position--;
                fileReader.seek(position);
                if (fileReader.readByte() == '\n') {
                    if (logger.isDebugEnabled()) {
                        logger.debug("find new line in file {}", lastFile);
                    }
                    String line = fileReader.readLine();

                    if (StringUtils.isBlank(line)) {
                        logger.info("find empty line in file [{}], continue", lastFile);
                        continue;
                    }
                    logger.info("last file message: {}", line);
                    Pair<String, Long> binglogFilePosition = initBinglogFilePosition(line);
                    binlogFilename = binglogFilePosition.getKey();
                    nextPosition = binglogFilePosition.getValue();
                    return;
                }
            }
            logger.info("read file [{}] completely, cannot find last read position", lastFile);
        } catch (IOException e) {
            logger.error("cannot find position from file [{}]", lastFile);
            throw e;
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }
    }

    private void initLastDataFromRocketMQ() throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("BINLOG_CONSUMER_GROUP");
        consumer.setNamesrvAddr(config.getMqNamesrvAddr());
        consumer.setMessageModel(MessageModel.valueOf("BROADCASTING"));
        consumer.start();

        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(config.getMqTopic());

        for (MessageQueue queue : queues) {
            if (queue != null) {
                Long offset = consumer.maxOffset(queue);
                if (offset > 0)
                    offset--;

                PullResult pullResult = consumer.pull(queue, "*", offset, 100);

                if (pullResult.getPullStatus() == PullStatus.FOUND) {
                    List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                    MessageExt msg = msgFoundList.get(msgFoundList.size() - 1);
                    WrapMessage wrapMessage = MessageSerde.deserialize(msg);
                    String messageStr = new String(wrapMessage.getMessageBody(), "UTF-8");
                    if (StringUtils.isBlank(messageStr)) {
                        continue;
                    }
                    String[] split = messageStr.split("\n");

                    String json = split[split.length-1];
                    logger.info("topic: {}, queue: {}, last message is [{}] ", config.getMqTopic(), queue.getQueueId(), json);
                    Pair<String, Long> binglogFilePosition = initBinglogFilePosition(json);

                    if (StringUtils.isBlank(binlogFilename)
                            || (binlogFilename.compareTo(binglogFilePosition.getKey()) <= 0
                            && nextPosition < binglogFilePosition.getValue())) {
                        binlogFilename = binglogFilePosition.getKey();
                        nextPosition = binglogFilePosition.getValue();
                    }
                }
            }
        }
        consumer.shutdown();
    }

    private Pair<String, Long> initBinglogFilePosition(String json) {
        JSONObject js = JSON.parseObject(json);
        String binFileName = (String) js.get("binlogFilename");
        Long position = js.getLong("nextPosition");
        return MutablePair.of(binFileName, position);
    }
}
