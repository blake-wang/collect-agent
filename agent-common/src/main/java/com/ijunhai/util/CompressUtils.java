package com.ijunhai.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class CompressUtils {

    private static final Logger logger = LoggerFactory.getLogger(CompressUtils.class);

    public static byte[] zip(byte[] data, int level) {
        byte[] b = null;
        ByteArrayOutputStream bos = null;
        ZipOutputStream zip = null;
        try {
            bos = new ByteArrayOutputStream();
            zip = new ZipOutputStream(bos);
            zip.setLevel(level);
            ZipEntry entry = new ZipEntry("zip");
            entry.setSize(data.length);
            zip.putNextEntry(entry);
            zip.write(data);
            zip.closeEntry();
            b = bos.toByteArray();
        } catch (Exception ex) {
            logger.error("zip failed", ex);
        } finally {
            try { if (zip != null) zip.close(); } catch (IOException e) {}
            try { if (bos != null) bos.close(); } catch (IOException e) {}
        }
        return b;
    }


    public static byte[] unZip(byte[] data) {
        byte[] b = null;
        ByteArrayInputStream bis = null;
        ZipInputStream zip = null;
        try {
            bis = new ByteArrayInputStream(data);
            zip = new ZipInputStream(bis);
            while (zip.getNextEntry() != null) {
                byte[] buf = new byte[1024];
                int num = 0;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                while ((num = zip.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, num);
                }
                b = baos.toByteArray();
                baos.flush();
                baos.close();
            }
        } catch (Exception ex) {
            logger.error("unzip failed", ex);
        } finally {
            try { if (zip != null) zip.close(); } catch (IOException e) {}
            try { if (bis != null) bis.close(); } catch (IOException e) {}
        }
        return b;
    }

}
