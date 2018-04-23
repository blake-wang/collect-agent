package com.ijunhai;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class PositionStorageTest {

    private Path tmpDir;
    private File derbyDir;
    private PositionStorage positionStorage;

    @Before
    public void setUp() throws Exception {
        tmpDir = Files.createTempDirectory("derbyTest");
        derbyDir = new File(tmpDir.toFile(), "derby");
        positionStorage = new PositionStorage(derbyDir);
        positionStorage.start();
    }

    @After
    public void tearDown() throws Exception {
        positionStorage.stop();
        FileUtils.forceDelete(tmpDir.toFile());
    }

    @Test
    public void test() {
        FileInfo expect = new FileInfo("localost", "/test", Long.MAX_VALUE, Long.MAX_VALUE);
        try {
            positionStorage.getDerbyHelper().retryTransaction(new TransactionCallback<Integer>() {
                @Override
                public Integer inTransaction(Handle handle, TransactionStatus status) throws Exception {
                    return positionStorage.insertFileInfo(handle, Long.MAX_VALUE, expect);
                }
            }, 3, 3);
        } catch (Exception ex) {

        }

        FileInfo fileInfo =  positionStorage.getFileInfo(Long.MAX_VALUE);
        Assert.assertEquals(expect, fileInfo);
    }

}
