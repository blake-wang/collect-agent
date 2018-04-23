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

package com.ijunhai.rocketmq.mysql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;


public class Config {

    private String mysqlAddr;
    private Integer mysqlPort;
    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlDatabase;
    private String mysqlTableWhiteList;

    private String mqNamesrvAddr;
    private String mqTopic;

    private String startType = "DEFAULT";
    private String binlogFilename;
    private Long nextPosition;
    private Integer maxTransactionRows = 100;

    private String emitter;

    private String storageType = "FILE";

    private String dataTempPath;
    private Integer dataReservedDays;
    private String fileNamePrefix = "";

    public void load() throws IOException {

        InputStream in = Config.class.getClassLoader().getResourceAsStream("mysql-agent.properties");
        Properties properties = new Properties();
        properties.load(in);

        properties2Object(properties, this);

    }

    public void load(String filePath) throws IOException {

        InputStream in = new FileInputStream(filePath);
        Properties properties = new Properties();
        properties.load(in);

        properties2Object(properties, this);

    }

    private void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public String getMysqlAddr() {
        return mysqlAddr;
    }

    public void setMysqlAddr(String mysqlAddr) {
        this.mysqlAddr = mysqlAddr;
    }

    public Integer getMysqlPort() {
        return mysqlPort;
    }

    public void setMysqlPort(Integer mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public String getMysqlDatabase() {
        return mysqlDatabase;
    }

    public void setMysqlDatabase(String mysqlDatabase) {
        this.mysqlDatabase = mysqlDatabase;
    }

    public String getMysqlTableWhiteList() {
        return mysqlTableWhiteList;
    }

    public void setMysqlTableWhiteList(String mysqlTableWhiteList) {
        this.mysqlTableWhiteList = mysqlTableWhiteList;
    }

    public String getMqNamesrvAddr() {
        return mqNamesrvAddr;
    }

    public void setMqNamesrvAddr(String mqNamesrvAddr) {
        this.mqNamesrvAddr = mqNamesrvAddr;
    }

    public String getMqTopic() {
        return mqTopic;
    }

    public void setMqTopic(String mqTopic) {
        this.mqTopic = mqTopic;
    }

    public String getStartType() {
        return startType;
    }

    public void setStartType(String startType) {
        this.startType = startType;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public Long getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public Integer getMaxTransactionRows() {
        return maxTransactionRows;
    }

    public void setMaxTransactionRows(Integer maxTransactionRows) {
        this.maxTransactionRows = maxTransactionRows;
    }

    public String getEmitter() {
        return emitter;
    }

    public void setEmitter(String emitter) {
        this.emitter = emitter;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getDataTempPath() {
        return dataTempPath;
    }

    public void setDataTempPath(String dataTempPath) {
        this.dataTempPath = dataTempPath;
    }

    public Integer getDataReservedDays() {
        return dataReservedDays;
    }

    public void setDataReservedDays(Integer dataReservedDays) {
        this.dataReservedDays = dataReservedDays;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }
}