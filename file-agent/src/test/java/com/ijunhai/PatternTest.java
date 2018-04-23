package com.ijunhai;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternTest {

    @Test
    public void test() {
        String a ="Agent_(active|login|order)(?!.*log).*";
        ArrayList<String> list = Lists.newArrayList("Agent_active-dasjdsa",
                "Agent_login-dasjdsa",
                "Agent_order-dasjdsa",
                "Agent_active-hhh-log-dasd");
        Pattern compile = Pattern.compile(a);

        Matcher matcher1 = compile.matcher("Agent_active-dasjdsa");
        Matcher matcher2 = compile.matcher("Agent_login-dasjdsa");
        Matcher matcher3 = compile.matcher("Agent_order-dasjdsa");
        Matcher matcher4 = compile.matcher("Agent_active-hhh-log-dasd");

        Assert.assertEquals(true, matcher1.matches());
        Assert.assertEquals(true, matcher2.matches());
        Assert.assertEquals(true, matcher3.matches());
        Assert.assertEquals(false, matcher4.matches());
    }
}
