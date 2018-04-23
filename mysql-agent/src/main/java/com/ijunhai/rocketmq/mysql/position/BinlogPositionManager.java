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

package com.ijunhai.rocketmq.mysql.position;

import com.ijunhai.rocketmq.mysql.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.exception.MQClientException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BinlogPositionManager {
    private DataSource dataSource;
    private Config config;

    private String binlogFilename;
    private Long nextPosition;
    private BinlogPositionStorage binlogPositionStorage;

    public BinlogPositionManager(Config config, DataSource dataSource) throws MQClientException {
        this.config = config;
        this.dataSource = dataSource;
        this.binlogPositionStorage = new BinlogPositionStorage(config);
    }

    public void initBeginPosition() throws Exception {
        if (config.getStartType() == null || "DEFAULT".equals(config.getStartType())) {
            initPositionDefault();

        } else if ("NEW_EVENT".equals(config.getStartType())) {
            initPositionFromBinlogTail();

        } else if ("LAST_PROCESSED".equals(config.getStartType())) {
            initPositionFromStorage();

        } else if ("SPECIFIED".equals(config.getStartType())) {
            binlogFilename = config.getBinlogFilename();
            nextPosition = config.getNextPosition();

        }

        if (binlogFilename == null || nextPosition == null) {
            throw new Exception("binlogFilename | nextPosition is null.");
        }
    }

    public void clear() {
        binlogFilename = null;
        nextPosition = null;
    }

    private void initPositionDefault() throws Exception {
        if (StringUtils.isNoneBlank(binlogFilename) && nextPosition > 0) {
            return;
        }
        initPositionFromStorage();

        if (binlogFilename == null || nextPosition == null) {
            initPositionFromBinlogTail();
        }

    }

    private void initPositionFromStorage() throws Exception {

        Pair<String, Long> lastBinglogFilePosition =
                binlogPositionStorage.getLastBinglogFilePosition();
        this.binlogFilename = lastBinglogFilePosition.getKey();
        this.nextPosition = lastBinglogFilePosition.getValue();
    }

    private void initPositionFromBinlogTail() throws SQLException {
        String sql = "SHOW MASTER STATUS";

        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(sql);

            while (rs.next()) {
                binlogFilename = rs.getString("File");
                nextPosition = rs.getLong("Position");
            }
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (Exception ignore) {}
            }
            if (statement != null) {
                try { statement.close(); } catch (Exception ignore) {}
            }
            if (conn != null) {
                try { conn.close(); } catch (Exception ignore) {}
            }
        }

    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public Long getPosition() {
        return nextPosition;
    }
}
