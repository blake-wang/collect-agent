package com.ijunhai.metric;

import com.google.common.collect.Maps;

import java.util.Map;

public class CollectEvent {

    public final static String HOST = "host.ip";

    public static Builder builder(String host, String datasource, String fileName, String type) {
        return new Builder(host, datasource, fileName, type);
    }

    private long timestamp;
    private String host;
    private String datasource;
    private String fileName;
    private String type;
    private String metricName;
    private long value;
    private Map<String, Object> tags;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public Map<String, Object> getTags() {
        return tags;
    }

    public void setTags(Map<String, Object> tags) {
        this.tags = tags;
    }

    public static class Builder {

        private final Map<String, Object> userDims = Maps.newHashMap();

        private String host;
        private String datasource;
        private String fileName;
        private String type;

        public Builder(String host, String datasource, String fileName, String type) {
            this.host = host;
            this.datasource = datasource;
            this.fileName = fileName;
            this.type = type;
        }

        public Builder setDimension(String dim, String value) {
            userDims.put(dim, value);
            return this;
        }

        public CollectEvent build(String metricName, long value) {
            return build(System.currentTimeMillis(), metricName, value);
        }

        public CollectEvent build(long timestamp, String metricName, long value) {
            CollectEvent event = new CollectEvent();
            event.setHost(host);
            event.setDatasource(datasource);
            event.setFileName(fileName);
            event.setType(type);
            event.setTags(userDims);
            event.setTimestamp(timestamp);
            event.setMetricName(metricName);
            event.setValue(value);
            return event;
        }

    }
}
