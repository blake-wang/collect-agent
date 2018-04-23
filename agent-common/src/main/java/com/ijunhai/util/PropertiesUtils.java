package com.ijunhai.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertiesUtils {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);

    private static Properties properties=new Properties();

    public static void init() {
        try {
            logger.info("config file {}", "file-agent.properties");
            properties.load(PropertiesUtils.class.getClassLoader().getResourceAsStream("file-agent.properties"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void init(String filePath) {
        try {
            logger.info("config file {}", filePath);
            properties.load(new FileInputStream(filePath));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static String get(String key){
        return (String) properties.get(key);
    }

    public static Integer getInt(String key){
        try {
            return Integer.parseInt(get(key));
        } catch (Exception ex) {
            return null;
        }
    }
    public static int getInt(String key, int defaultValue){
        try {
            return Integer.parseInt(get(key));
        } catch (Exception ex) {
            return defaultValue;
        }
    }


    public static Long getLong(String key){
        try {
            return Long.parseLong(get(key));
        } catch (Exception ex) {}
        return null;
    }

    public static long getLong(String key, long defaultValue){
        try {
            return Long.parseLong(get(key));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public static Object get(String key, Object defaultValue){
        return properties.getOrDefault(key, defaultValue);
    }


}