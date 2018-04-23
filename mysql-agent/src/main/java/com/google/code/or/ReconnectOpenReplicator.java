package com.google.code.or;

import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserListener;
import com.ijunhai.rocketmq.mysql.position.BinlogPositionManager;
import com.ijunhai.rocketmq.mysql.productor.RocketMQProducer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ReconnectOpenReplicator extends OpenReplicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectOpenReplicator.class);

    private final BinlogPositionManager binlogPositionManager;
//    private final RocketMQProducer rocketMQProducer;


    public ReconnectOpenReplicator(final BinlogPositionManager binlogPositionManager) {
        this.binlogPositionManager = binlogPositionManager;
//        this.rocketMQProducer = rocketMQProducer;
    }

    public void start() throws Exception {

        if(super.running.get()) {
            return;
        }

        binlogPositionManager.initBeginPosition();

        super.setBinlogFileName(binlogPositionManager.getBinlogFilename());
        super.setBinlogPosition(binlogPositionManager.getPosition());
//        rocketMQProducer.setBinlogFilename(super.binlogFileName);
//        rocketMQProducer.setPosition(super.binlogPosition);

        //
        super.transport = super.getDefaultTransport();
        super.transport.connect(super.host, super.port);

        //
        if(super.binlogParser == null)
            super.binlogParser = getDefaultBinlogParser();

        setupChecksumState();
        setupHeartbeatPeriod();
        setupSlaveUUID();
        dumpBinlog();

        super.binlogParser.setTransport(this.transport);
        super.binlogParser.setBinlogFileName(super.binlogFileName);
        super.binlogParser.setEventListener(super.binlogEventListener);
        super.binlogParser.setParserListeners(null);
        super.binlogParser.addParserListener(new BinlogParserListener.Adapter() {
            @Override
            public void onStart(BinlogParser parser) {
                running.set(true);
                LOGGER.info("started.");
            }

            @Override
            public void onStop(BinlogParser parser) {
                stopQuietly(0, TimeUnit.MILLISECONDS);
                binlogPositionManager.clear();
                LOGGER.info("stoped");
            }
        });
        super.binlogParser.start();
    }

}
