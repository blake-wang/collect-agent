package com.ijunhai.serde;

import org.apache.rocketmq.common.message.Message;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.ijunhai.serde.MessageSerde.COMPRESS_SIZE_THRESHOLD;

public class MessageSerdeTest {

    private final String TOPIC = "testTopic";
    private final String DATA_SOURCE = "testSys";
    private final String IP = "127.0.0.1";
    private final String FILE_PATH = "/a/bssss/ddfdfsfd/sdadsa";
	@Test
	public void testCompressSerde() {
		ByteBuffer messageByteBuffer = ByteBuffer.allocateDirect(COMPRESS_SIZE_THRESHOLD);
		byte[] bytes = new byte[COMPRESS_SIZE_THRESHOLD];
		int lineCount = 0;
		for (int i =0; i < COMPRESS_SIZE_THRESHOLD; i++) {
			if (i % 1024 == 0) {
			    bytes[i] = '\n';
                lineCount++;
			} else {
			    bytes[i] = 'a';
		    }
        }
		messageByteBuffer.put(bytes);
		Message message = MessageSerde.serialize(TOPIC, IP, FILE_PATH, messageByteBuffer, lineCount, DATA_SOURCE);

        WrapMessage wrapMessage = MessageSerde.deserialize(message);

        Assert.assertEquals(lineCount, wrapMessage.getLineCount());
        Assert.assertEquals(DATA_SOURCE, wrapMessage.getDataSystem());
        Assert.assertEquals(FILE_PATH, wrapMessage.getFilePath());
        Assert.assertArrayEquals(bytes, wrapMessage.getMessageBody());
    }

    @Test
    public void testUnCompressSerde() {
        ByteBuffer messageByteBuffer = ByteBuffer.allocateDirect(COMPRESS_SIZE_THRESHOLD);
        byte[] bytes = new byte[64];
        int lineCount = 0;
        for (int i =0; i < 64; i++) {
            if (i % 4 == 0) {
                bytes[i] = '\n';
                lineCount++;
            } else {
                bytes[i] = 'a';
            }
        }
        messageByteBuffer.put(bytes);
        Message message = MessageSerde.serialize(TOPIC, IP, FILE_PATH, messageByteBuffer, lineCount, DATA_SOURCE);

        WrapMessage wrapMessage = MessageSerde.deserialize(message);

        Assert.assertEquals(lineCount, wrapMessage.getLineCount());
        Assert.assertEquals(DATA_SOURCE, wrapMessage.getDataSystem());
        Assert.assertEquals(IP, wrapMessage.getIp());
        Assert.assertEquals(FILE_PATH, wrapMessage.getFilePath());
        Assert.assertArrayEquals(bytes, wrapMessage.getMessageBody());
    }
}
