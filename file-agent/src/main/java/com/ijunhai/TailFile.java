package com.ijunhai;

import com.ijunhai.metric.CollectMetrics;
import com.ijunhai.metric.MetricType;
import com.ijunhai.monitor.CollectMonitor;
import com.ijunhai.monitor.MonitorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TailFile {
    private static final Logger logger = LoggerFactory.getLogger(TailFile.class);
    // \n
    private static final byte BYTE_NL = (byte) 10;
    private static final byte BYTE_R = (byte) 13;
    private static final int END = 0;

    private Long position;
    private Integer bufferSize;
    private final RandomAccessFile randomAccessFile;
    private final File file;
    private final String filePath;
    private AtomicInteger currentLineCount = new AtomicInteger();
    private AtomicLong readedCount = new AtomicLong();

    private long maxTailTime;

    private FileInfo fileInfo;

    private long lastTailTime = System.currentTimeMillis();

    public TailFile(FileInfo fileInfo, Integer bufferSize, long maxTailSeconds) throws IOException {
        this.position = fileInfo.getPosition() == null ? 0 : fileInfo.getPosition();
        this.bufferSize = bufferSize;
        this.file = new File(fileInfo.getFilePath());
        this.filePath = fileInfo.getFilePath();
        this.randomAccessFile = new RandomAccessFile(file, "r");
        this.randomAccessFile.seek(this.position);
        this.maxTailTime = maxTailSeconds*1000;
        this.fileInfo = fileInfo;
        this.readedCount.set(fileInfo.getReadedLineCount());
    }

    public synchronized void tail(ByteBuffer readedBuffer) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("tail file [{}]", filePath);
        }
        currentLineCount.set(0);

        byte[] buffer;
        if (randomAccessFile.length() - position >= bufferSize) {
            buffer = new byte[bufferSize];
        } else {
            buffer = new byte[(int) (randomAccessFile.length() - position + 1)];
        }
        this.randomAccessFile.seek(this.position);
        randomAccessFile.read(buffer, 0, buffer.length);

        int bufferPosition = 0;

        // if start with \n or \r\n, then ignore
        int i = 0;
        if (buffer[0] == BYTE_NL) {
            i = 1;
        }
        if (buffer[0] == BYTE_R && buffer[1] == BYTE_NL) {
            i = 2;
        }
        for (; i < buffer.length; i++) {
            if (buffer[i] == BYTE_NL) {
                bufferPosition = i;
                currentLineCount.incrementAndGet();
            }
        }
        if (buffer[buffer.length - 1] == END && buffer[buffer.length - 2] != BYTE_NL) {
            bufferPosition = buffer.length - 2;
            currentLineCount.incrementAndGet();
            readedBuffer.put(buffer, 0, bufferPosition+1);
            readedBuffer.put(BYTE_NL);
        } else {
            readedBuffer.put(buffer, 0, bufferPosition+1);
        }
        position += bufferPosition+1;
        readedCount.getAndAdd(currentLineCount.intValue());
        fileInfo.setReadedLineCount(readedCount.get());
        fileInfo.setPosition(position);
        logger.info("File [{}] has read {} lines", filePath, fileInfo.getReadedLineCount());
        lastTailTime = System.currentTimeMillis();
    }

    public File getFile() {
        return file;
    }

    public int getCurrentLineCount() {
        return currentLineCount.intValue();
    }

    public int getCurrentLineCountAndSet(int value) {
        return currentLineCount.getAndSet(value);
    }

    public long getPosition() {
        return position;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public boolean isComplete() throws IOException {
        if (randomAccessFile.length() == position) {
            return true;
        }
        return false;
    }

    public boolean needRead() {
        try {
            boolean isTimeOut = System.currentTimeMillis() - lastTailTime >= maxTailTime;
            if (randomAccessFile.length() - position == 0 && isTimeOut) {
                lastTailTime = System.currentTimeMillis();
            }

            if (randomAccessFile.length() - position >= bufferSize ||
                    (randomAccessFile.length() - position > 0 && isTimeOut)) {
                return true;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("File [{}] length: {}, position: {}, isTimeOut: {}",
                        filePath, randomAccessFile.length(), position, isTimeOut);
            }
            return false;
        } catch (IOException ignore) {
        }
        return false;
    }
    public void close() {
        try {
            randomAccessFile.close();
        } catch (IOException e) {}
    }
}
