package com.ijunhai.util;

import org.apache.commons.cli.*;

public final class CommandLineUtils {

    private CommandLineUtils() {}

    private final static CommandLineParser parser = new DefaultParser();

    public static CommandLine genCommandLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("c", "config", true, "agent config file");
        options.addOption("l", "logback", true, "logback config file");
        return parser.parse(options, args);
    }
}
