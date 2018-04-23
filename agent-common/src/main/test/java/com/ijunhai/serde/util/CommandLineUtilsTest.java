package com.ijunhai.serde.util;

import com.ijunhai.util.CommandLineUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class CommandLineUtilsTest {

    @Test
    public void test() throws ParseException {
        String[] args = new String[]{"-c", "a.properties"};
        CommandLine commandLine = CommandLineUtils.genCommandLine(args);
        Assert.assertTrue(commandLine.hasOption('c'));
        Assert.assertTrue(commandLine.hasOption("config"));
        Assert.assertEquals(commandLine.getOptionValue('c'), "a.properties");
        Assert.assertEquals(commandLine.getOptionValue("config"), "a.properties");
    }
}
