package com.ijunhai;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.ijunhai.derby.DerbyHelper;
import org.apache.commons.lang3.StringUtils;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.IntegerColumnMapper;
import org.skife.jdbi.v2.util.StringColumnMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class PositionStorage {

    private static final Logger logger = LoggerFactory.getLogger(PositionStorage.class);

    private static final String tableName = "POSITION";

    private File positionFile;
    private DerbyHelper derbyHelper;

    public PositionStorage(File positionFile) {
        this.positionFile = positionFile;

    }

    public void start() {
        derbyHelper = new DerbyHelper(positionFile);
        derbyHelper.start();
        createPositionTable();
    }

    public DerbyHelper getDerbyHelper() {
        return derbyHelper;
    }


    public FileInfo getFileInfo(long inode) {
        return derbyHelper.retryWithHandle(new HandleCallback<FileInfo>() {
            @Override
            public FileInfo withHandle(Handle handle) throws Exception {
                return getFileInfo(handle, inode);
            }
        });
    }

    public FileInfo getFileInfo(Handle handle, long inode) {
        String fileInfoStr = handle.createQuery("select fileInfo from " + tableName + " where inode = :inode")
                .setFetchSize(1)
                .bind("inode", inode)
                .map(StringColumnMapper.INSTANCE)
                .first();

        if (StringUtils.isBlank(fileInfoStr)) {
            return null;
        }

        return JSON.parseObject(fileInfoStr, FileInfo.class);
    }

    public boolean isInodeExist(Handle handle, long inode) {
        return handle.createQuery("select count(*) from " + tableName + " where inode = :inode")
                .bind("inode", inode)
                .map(IntegerColumnMapper.PRIMITIVE)
                .first() > 0;

    }


    public int updateFileInfo(long inode, FileInfo fileInfo) {
        return derbyHelper.retryWithHandle(new HandleCallback<Integer>() {
            @Override
            public Integer withHandle(Handle handle) throws Exception {
                return updateFileInfo(handle, inode, fileInfo);
            }
        });
    }


    public int updateFileInfo(Handle handle, long inode, FileInfo fileInfo) {
        return handle.createStatement("update " + tableName + " set fileInfo = :fileInfo where inode = :inode")
                .bind("fileInfo", fileInfo.toString())
                .bind("inode", inode)
                .execute();
    }

    public int insertFileInfo(long inode, FileInfo fileInfo) {
        return derbyHelper.retryWithHandle(new HandleCallback<Integer>() {
            @Override
            public Integer withHandle(Handle handle) throws Exception {
                return handle.createStatement("insert into " + tableName + " values (:inode, :fileInfo)")
                        .bind("fileInfo", fileInfo.toString())
                        .bind("inode", inode)
                        .execute();
            }
        });

    }

    public int insertFileInfo(Handle handle, long inode, FileInfo fileInfo) {
        return handle.createStatement("insert into " + tableName + " values (:inode, :fileInfo)")
                .bind("fileInfo", fileInfo.toString())
                .bind("inode", inode)
                .execute();
    }

    public int deleteFileInfo(long inode) {
        return derbyHelper.retryWithHandle(new HandleCallback<Integer>() {
            @Override
            public Integer withHandle(Handle handle) throws Exception {
                return deleteFileInfo(handle, inode);
            }
        });
    }

    public int deleteFileInfo(Handle handle, long inode) {
        return handle.createStatement("delete from " + tableName + " where inode = :inode")
                .bind("inode", inode)
                .execute();
    }

    public boolean tableExists(Handle handle, String tableName)
    {
        return !handle.createQuery("select * from SYS.SYSTABLES where tablename = :tableName")
                .bind("tableName", tableName.toUpperCase())
                .list()
                .isEmpty();
    }

    private void createPositionTable()
    {
        derbyHelper.createTable(
                tableName,
                ImmutableList.of(
                        String.format(
                                "CREATE TABLE %1$s (inode bigint, fileInfo clob, PRIMARY KEY (inode))",
                                tableName
                        )
//                        String.format("ALTER TABLE %1$s ADD PRIMARY KEY (inode)", tableName)
                )
        );
    }

    public void stop() {

//        try { slectStatement.close(); } catch (SQLException ignored) {}
//        try { updateStatement.close(); } catch (SQLException ignored) {}
//        try { insertStatement.close(); } catch (SQLException ignored) {}
//        try { conn.close(); } catch (SQLException ignored) {}
    }
}
