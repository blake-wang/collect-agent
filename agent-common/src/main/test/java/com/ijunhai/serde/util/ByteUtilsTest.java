package com.ijunhai.serde.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ByteUtilsTest {
    @Test
    public void testWriteAndReadInt() {
        int i = new Random().nextInt();
        byte[] bytes = ByteUtils.writeInt(i);
        int i1 = ByteUtils.readInt(bytes, 0);
        Assert.assertEquals(i, i1);
    }
}
