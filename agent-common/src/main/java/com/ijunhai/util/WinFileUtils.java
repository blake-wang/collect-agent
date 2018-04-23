package com.ijunhai.util;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WinFileUtils {

    public  static WinFileUtils getWinFile(){
        return  new WinFileUtils();
    }
    private static Logger logger = LoggerFactory.getLogger(WinFileUtils.class);

    public static String getFileId(String filepath) {

        final int FILE_SHARE_READ = (0x00000001);
        final int OPEN_EXISTING = (3);
        final int GENERIC_READ = (0x80000000);
        final int FILE_ATTRIBUTE_ARCHIVE = (0x20);

        WinBase.SECURITY_ATTRIBUTES attr = null;
        com.ijunhai.util.Kernel32.BY_HANDLE_FILE_INFORMATION lpFileInformation = new com.ijunhai.util.Kernel32.BY_HANDLE_FILE_INFORMATION();
        HANDLE hFile = null;

        hFile = Kernel32.INSTANCE.CreateFile(filepath, 0,
                FILE_SHARE_READ, attr, OPEN_EXISTING, FILE_ATTRIBUTE_ARCHIVE,
                null);
        String ret = "0";
        if (Kernel32.INSTANCE.GetLastError() == 0) {

            com.ijunhai.util.Kernel32.INSTANCE
                    .GetFileInformationByHandle(hFile, lpFileInformation);

            ret = lpFileInformation.dwVolumeSerialNumber.toString()
                    + lpFileInformation.nFileIndexLow.toString();

            Kernel32.INSTANCE.CloseHandle(hFile);

            if (Kernel32.INSTANCE.GetLastError() == 0) {
                logger.debug("inode:" + ret);
                return ret;
            } else {
                logger.error("Close file error:{}", filepath);
                throw new RuntimeException("Close file error:" + filepath);
            }
        } else {
            if (hFile != null) {
                Kernel32.INSTANCE.CloseHandle(hFile);
            }
            logger.error("Open file error:{}", filepath);
            throw new RuntimeException("Open file error:" + filepath);
        }

    }

}


