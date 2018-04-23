package com.ijunhai;

import com.ijunhai.util.UnicodeUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnicodeUtilsTest {

    private String encode = "\\u4f20\\u5947SF";
    private String decode = "传奇SF";

    @Test
    public void test() {
        Assert.assertEquals(encode, UnicodeUtils.string2Unicode(decode));
        Assert.assertEquals(decode, UnicodeUtils.unicode2String(encode));

    }
}
