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

package com.ijunhai.rocketmq.mysql.binlog;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.fastjson.JSON;
import com.google.code.or.OpenReplicator;
import com.google.code.or.ReconnectOpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;
import com.ijunhai.rocketmq.mysql.Config;
import com.ijunhai.rocketmq.mysql.Replicator;
import com.ijunhai.rocketmq.mysql.position.BinlogPosition;
import com.ijunhai.rocketmq.mysql.position.BinlogPositionManager;
import com.ijunhai.rocketmq.mysql.schema.Schema;
import com.ijunhai.rocketmq.mysql.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class EventProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    private final Replicator replicator;
    private final Config config;
    private final ScheduledExecutorService reconnectExecutorService;

    private volatile boolean isProcessing = true;

    private DataSource dataSource;

    private BinlogPositionManager binlogPositionManager;

    private BlockingQueue<BinlogEventV4> queue = new LinkedBlockingQueue<>(100);

    private OpenReplicator openReplicator;

    private EventListener eventListener;

    private Schema schema;

    private Map<Long, Table> tableMap = new HashMap<>();

    private Transaction transaction;

    private List<String> whiteTaleList;

    public EventProcessor(Replicator replicator) {

        this.replicator = replicator;
        this.config = replicator.getConfig();

        this.reconnectExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() throws Exception {
        isProcessing = true;
        initDataSource();

        binlogPositionManager = new BinlogPositionManager(config, dataSource);

        schema = new Schema(dataSource, config.getMysqlDatabase(), whiteTaleList);
        schema.load();

        eventListener = new EventListener(queue);
        openReplicator = new ReconnectOpenReplicator(binlogPositionManager);
        openReplicator.setBinlogEventListener(eventListener);
        openReplicator.setHost(config.getMysqlAddr());
        openReplicator.setPort(config.getMysqlPort());
        openReplicator.setUser(config.getMysqlUsername());
        openReplicator.setPassword(config.getMysqlPassword());
        openReplicator.setStopOnEOF(false);
        openReplicator.setHeartbeatPeriod(1f);
        openReplicator.setLevel2BufferSize(50 * 1024 * 1024);
        openReplicator.setServerId(1001);

        openReplicator.start();

        reconnectExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (!openReplicator.isRunning()) {
                    try {
                        openReplicator.start();
                        if (!openReplicator.isRunning()) {
                            LOGGER.error("cannot start replicator, will try again 5 seconds later");
                        }
                    } catch (Exception e) {
                        LOGGER.error("cannot start replicator, err: " + e.getMessage());
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
        doProcess();
    }

    public void stopOpenReplicator() {
        try {
            isProcessing = false;
            openReplicator.stop(0, TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {
        }
    }

    private void doProcess() {

        while (isProcessing) {

            try {
                BinlogEventV4 event = queue.poll(100, TimeUnit.MILLISECONDS);
                if (event == null) {
//                    checkConnection();
                    continue;
                }

                switch (event.getHeader().getEventType()) {
                    case MySQLConstants.TABLE_MAP_EVENT:
                        processTableMapEvent(event);
                        break;

                    case MySQLConstants.WRITE_ROWS_EVENT:
                        processWriteEvent(event);
                        break;

                    case MySQLConstants.WRITE_ROWS_EVENT_V2:
                        processWriteEventV2(event);
                        break;

                    case MySQLConstants.UPDATE_ROWS_EVENT:
                        processUpdateEvent(event);
                        break;

                    case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                        processUpdateEventV2(event);
                        break;

                    case MySQLConstants.DELETE_ROWS_EVENT:
                        processDeleteEvent(event);
                        break;

                    case MySQLConstants.DELETE_ROWS_EVENT_V2:
                        processDeleteEventV2(event);
                        break;

                    case MySQLConstants.QUERY_EVENT:
                        processQueryEvent(event);
                        break;

                    case MySQLConstants.XID_EVENT:
                        processXidEvent(event);
                        break;
                    default:
                        break;

                }
            } catch (Exception e) {
                LOGGER.error("Binlog process error.", e);
            }

        }
    }

    private void checkConnection() throws Exception {

        if (!openReplicator.isRunning()) {
            BinlogPosition binlogPosition = replicator.getNextBinlogPosition();
            if (binlogPosition != null) {
                openReplicator.setBinlogFileName(binlogPosition.getBinlogFilename());
                openReplicator.setBinlogPosition(binlogPosition.getPosition());
            }

            while (true) {
                try {
                    openReplicator.start();
                    break;
                } catch (Exception ignore) {
                    Thread.sleep(1000);
                }
            }

        }
    }

    private void processTableMapEvent(BinlogEventV4 event) {
        TableMapEvent tableMapEvent = (TableMapEvent) event;
        String dbName = tableMapEvent.getDatabaseName().toString();
        String tableName = tableMapEvent.getTableName().toString();

        if (dbName.toLowerCase().equals(config.getMysqlDatabase()) && whiteTaleList.contains(tableName.toLowerCase())) {
            LOGGER.debug("find table [{}.{}]", dbName, tableName);
            Long tableId = tableMapEvent.getTableId();
            Table table = schema.getTable(dbName, tableName);
            tableMap.put(tableId, table);
        }
    }

    private void processWriteEvent(BinlogEventV4 event) {
        WriteRowsEvent writeRowsEvent = (WriteRowsEvent) event;
        Long tableId = writeRowsEvent.getTableId();
        List<Row> list = writeRowsEvent.getRows();

        for (Row row : list) {
            addRow("INSERT", tableId, row);
        }
    }

    private void processWriteEventV2(BinlogEventV4 event) {
        WriteRowsEventV2 writeRowsEventV2 = (WriteRowsEventV2) event;
        Long tableId = writeRowsEventV2.getTableId();
        List<Row> list = writeRowsEventV2.getRows();

        for (Row row : list) {
            addRow("INSERT", tableId, row);
        }

    }

    private void processUpdateEvent(BinlogEventV4 event) {
        UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) event;
        Long tableId = updateRowsEvent.getTableId();
        List<Pair<Row>> list = updateRowsEvent.getRows();

        for (Pair<Row> pair : list) {
            addRow("UPDATE", tableId, pair.getAfter());
        }
    }

    private void processUpdateEventV2(BinlogEventV4 event) {
        UpdateRowsEventV2 updateRowsEventV2 = (UpdateRowsEventV2) event;
        Long tableId = updateRowsEventV2.getTableId();
        List<Pair<Row>> list = updateRowsEventV2.getRows();

        for (Pair<Row> pair : list) {
            addRow("UPDATE", tableId, pair.getAfter());
        }
    }

    private void processDeleteEvent(BinlogEventV4 event) {
        DeleteRowsEvent deleteRowsEvent = (DeleteRowsEvent) event;
        Long tableId = deleteRowsEvent.getTableId();
        List<Row> list = deleteRowsEvent.getRows();

        for (Row row : list) {
            addRow("DELETE", tableId, row);
        }

    }

    private void processDeleteEventV2(BinlogEventV4 event) {
        DeleteRowsEventV2 deleteRowsEventV2 = (DeleteRowsEventV2) event;
        Long tableId = deleteRowsEventV2.getTableId();
        List<Row> list = deleteRowsEventV2.getRows();

        for (Row row : list) {
            addRow("DELETE", tableId, row);
        }

    }

    private static Pattern createTablePattern =
        Pattern.compile("^(CREATE|ALTER)\\s+TABLE", Pattern.CASE_INSENSITIVE);

    private void processQueryEvent(BinlogEventV4 event) {
        QueryEvent queryEvent = (QueryEvent) event;
        String sql = queryEvent.getSql().toString();

        if (createTablePattern.matcher(sql).find()) {
            schema.reset();
        }
    }

    private void processXidEvent(BinlogEventV4 event) {
        XidEvent xidEvent = (XidEvent) event;
        String binlogFilename = xidEvent.getBinlogFilename();
        Long position = xidEvent.getHeader().getNextPosition();
        Long xid = xidEvent.getXid();

        BinlogPosition binlogPosition = new BinlogPosition(binlogFilename, position);
        transaction.setNextBinlogPosition(binlogPosition);
        transaction.setXid(xid);

        replicator.commit(transaction, true);

        transaction = new Transaction(config.getMaxTransactionRows());

    }

    private void addRow(String type, Long tableId, Row row) {

        if (transaction == null) {
            transaction = new Transaction(config.getMaxTransactionRows());
        }

        Table t = tableMap.get(tableId);
        if (t != null) {

            while (true) {
                if (transaction.addRow(type, t, row)) {
                    replicator.getMonitor().getCollectMetrics().incrementLogReaded();
                    break;

                } else {
                    transaction.setNextBinlogPosition(replicator.getNextBinlogPosition());
                    replicator.commit(transaction, false);
                    transaction = new Transaction(config.getMaxTransactionRows());
                }
            }

        }
    }

    private void initDataSource() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("driverClassName", "com.mysql.jdbc.Driver");
//        map.put("driverClassName", "com.mysql.cj.jdbc.Driver");

        map.put("url", "jdbc:mysql://" + config.getMysqlAddr() + ":" + config.getMysqlPort() + "?verifyServerCertificate=false&autoReconnect=true");
        map.put("username", config.getMysqlUsername());
        map.put("password", config.getMysqlPassword());
        map.put("initialSize", "2");
        map.put("maxActive", "2");
        map.put("maxWait", "60000");
        map.put("timeBetweenEvictionRunsMillis", "60000");
        map.put("minEvictableIdleTimeMillis", "300000");
        map.put("validationQuery", "SELECT 1 FROM DUAL");
        map.put("testWhileIdle", "true");

        dataSource = DruidDataSourceFactory.createDataSource(map);

        whiteTaleList = (List<String>)JSON.parse(config.getMysqlTableWhiteList());
    }

    public Config getConfig() {
        return config;
    }

}
