package com.ijunhai.derby;

import java.io.IOException;
import java.io.OutputStream;


public class DerbyLogOutputStream {

    public static OutputStream disableLog() {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {

            }
        };
    }
}
