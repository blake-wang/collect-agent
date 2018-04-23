package com.ijunhai.rocketmq.mysql.productor;

import com.ijunhai.rocketmq.mysql.Config;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileProducer implements Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileProducer.class);

    private final static int DEFAULT_FILE_RESERVED_DAYS = 15;

    private Config config;

    private File dataTempDir;

    private FileWriter currentFileWriter;

    private LocalDate today;

    private final ScheduledExecutorService clearFilesThread;

    public FileProducer(Config config) throws IOException {
        this.config = config;
        dataTempDir = new File(config.getDataTempPath());
        if (!dataTempDir.exists() && !dataTempDir.mkdir()) {
            throw new IOException("Cannot create directory: " + dataTempDir);
        }
        today = LocalDate.now();
        currentFileWriter = new FileWriter(getFile(today), true);

        clearFilesThread = Executors.newScheduledThreadPool(1);
        clearFilesThread.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    clearFiles();
                } catch (Exception ignored) {
                }
            }
        }, TimeUnit.SECONDS.toMillis(3600), 3600, TimeUnit.SECONDS);
    }

    public synchronized boolean push(String json) {
        for (int i = 0; i < 3; i++) {
            try {
                if (LocalDate.now().isAfter(today)) {
                    today = LocalDate.now();
                    FileWriter tmpWriter = currentFileWriter;
                    tmpWriter.close();
                    currentFileWriter = new FileWriter(getFile(today), true);
                }
                if (StringUtils.isNotBlank(json)) {
                    currentFileWriter.write(json + "\n");
                    currentFileWriter.flush();
                }

                return true;
            } catch (IOException ex) {
                LOGGER.warn("Storage message failed, retried "+(i+1)+" times", ex);
            }
        }
        return false;
    }

    @Override
    public void stop() {
        try {
            currentFileWriter.flush();
        } catch (IOException ignored) {
        }
        try {
            currentFileWriter.close();
        } catch (IOException ignored) {
        }
    }

    private File getFile(LocalDate today) {
        String fileName = config.getFileNamePrefix() + "_" + today.toString();
        return new File(dataTempDir, fileName);
    }

    private final long datMillis = 24 * 3600 * 1000L;
    private void clearFiles() {
        int dataReservedDays = config.getDataReservedDays() == null ? DEFAULT_FILE_RESERVED_DAYS : config.getDataReservedDays();
        long millis = dataReservedDays * datMillis;
        if (dataTempDir.listFiles() == null) {
            return;
        }
        for (File file : dataTempDir.listFiles()) {
            if (System.currentTimeMillis() - file.lastModified() > millis) {
                try {
                    FileUtils.forceDelete(file);
                } catch (IOException ignored) {
                }
            }
        }
    }

}
