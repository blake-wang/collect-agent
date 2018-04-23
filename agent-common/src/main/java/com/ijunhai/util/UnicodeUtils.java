package com.ijunhai.util;

import org.apache.commons.lang3.StringUtils;

public class UnicodeUtils {

    public static String unicode2String(String unicode){
        if(StringUtils.isBlank(unicode))return unicode;
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (unicode.length() > i) {
            char c = unicode.charAt(i);
            if ('\\' == c && 'u' == unicode.charAt(i+1)) {
                sb.append((char)Integer.parseInt(unicode.substring(i+2, i+6), 16));
                i += 6;
            } else {
                sb.append(c);
                i += 1;
            }
        }
        return sb.toString();
    }

    public static String string2Unicode(String string) {

        if(StringUtils.isBlank(string))return null;
        StringBuffer unicode = new StringBuffer();

        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            unicode.append(c > 31 && c < 128 ? c : "\\u" + Integer.toHexString(c));
        }

        return unicode.toString();
    }

}
