package com.ijunhai;

import com.alibaba.fastjson.JSON;

public class FileInfo {
    private String ip;
    private String filePath;
    private Long position;
    private long readedLineCount;

    public FileInfo() {
    }

    public FileInfo(String ip, String filePath, Long position, long readedLineCount) {
        this.ip = ip;
        this.filePath = filePath;
        this.position = position;
        this.readedLineCount = readedLineCount;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public long getReadedLineCount() {
        return readedLineCount;
    }

    public void setReadedLineCount(long readedLineCount) {
        this.readedLineCount = readedLineCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileInfo fileInfo = (FileInfo) o;

        if (readedLineCount != fileInfo.readedLineCount) return false;
        if (ip != null ? !ip.equals(fileInfo.ip) : fileInfo.ip != null) return false;
        if (filePath != null ? !filePath.equals(fileInfo.filePath) : fileInfo.filePath != null) return false;
        return position != null ? position.equals(fileInfo.position) : fileInfo.position == null;
    }

    @Override
    public int hashCode() {
        int result = ip != null ? ip.hashCode() : 0;
        result = 31 * result + (filePath != null ? filePath.hashCode() : 0);
        result = 31 * result + (position != null ? position.hashCode() : 0);
        result = 31 * result + (int) (readedLineCount ^ (readedLineCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

}
