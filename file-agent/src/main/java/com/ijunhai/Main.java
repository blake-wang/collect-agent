package com.ijunhai;

import ch.qos.logback.core.joran.spi.JoranException;
import com.ijunhai.util.CommandLineUtils;
import com.ijunhai.util.LogBackConfigLoader;
import com.ijunhai.util.PropertiesUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ijunhai.TaildirConstants.*;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws MQClientException, IOException, ParseException, JoranException, SQLException, ClassNotFoundException {

        CommandLine commandLine = CommandLineUtils.genCommandLine(args);

        if (commandLine.hasOption('l')) {
            String filePath = commandLine.getOptionValue('l');
            logger.info("logback file [{}]", filePath);
            LogBackConfigLoader.load(filePath);
        }
        if (commandLine.hasOption('c')) {
            String filePath = commandLine.getOptionValue('c');
            PropertiesUtils.init(filePath);
        } else {
            PropertiesUtils.init();
        }

        String positionFile = PropertiesUtils.get(POSITION_FILE);
        String fileLastModifyStart = PropertiesUtils.get(FILE_LASTMODIFY_START);
        long fileLastModifyStamp = LocalDateTime.parse(fileLastModifyStart, DateTimeFormatter.ofPattern("yyyyMMddHHmm"))
                .atZone(ZoneId.of("+08:00")).toEpochSecond();
        long fileLastModifyMillis = fileLastModifyStamp * 1000;
        logger.info("position file {}", positionFile);
        final ReliableTaildirEventReader taildirReader = new ReliableTaildirEventReader(
                new File(positionFile),
                fileLastModifyMillis

        );
        List<Future> futures = taildirReader.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                taildirReader.stop();
            }
        }));

        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                logger.error("tailFilesThread get failed", e);
            }
        });
    }
}
