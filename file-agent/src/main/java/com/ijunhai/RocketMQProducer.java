package com.ijunhai;

import com.ijunhai.serde.MessageSerde;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public class RocketMQProducer {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQProducer.class);

    private DefaultMQProducer producer;

    private PositionStorage positionStorage;

    public RocketMQProducer(
            String nameServer,
            String producerGroup,
            int retryTimes,
            PositionStorage positionStorage
    ) throws MQClientException, SQLException, ClassNotFoundException {
        producer = producerGroup == null ? new DefaultMQProducer() : new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServer);
        producer.setRetryTimesWhenSendFailed(retryTimes);
        producer.start();

        this.positionStorage = positionStorage;
    }

    public synchronized boolean sendMesage(
            final String topic,
            final long inode,
            final FileInfo fileInfo,
            final ByteBuffer messageByteBuffer,
            final int messageCount,
            final String dataSystem
    ) {

        try {
            Message message = MessageSerde.serialize(topic, fileInfo.getIp(), fileInfo.getFilePath(), messageByteBuffer, messageCount, dataSystem);
            positionStorage.getDerbyHelper().retryTransaction(new TransactionCallback<Integer>() {
                @Override
                public Integer inTransaction(Handle handle, TransactionStatus status) throws Exception {
                    Integer updateCount = 0;
                    if (positionStorage.isInodeExist(handle, inode)) {
                        updateCount = positionStorage.updateFileInfo(handle, inode, fileInfo);
                    } else {
                        updateCount = positionStorage.insertFileInfo(handle, inode, fileInfo);
                    }

                    if (updateCount > 0) {
                        SendStatus sendStatus = producer.send(message).getSendStatus();
//                        SendStatus sendStatus = SendStatus.SEND_OK;
                        if (SendStatus.SEND_OK.equals(sendStatus)) {
                            // commit
                            messageByteBuffer.clear();
                            return updateCount;
                        } else {
                            throw new Exception("send message failed");
                        }
                    }
                    return 0;
                }
            }, 1 , 1);
            return true;
        } catch (Exception ex) {
            logger.error("send message fail", ex);
        }
        return false;
    }

    public void close() {
        producer.shutdown();
    }
}
