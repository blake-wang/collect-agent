package com.ijunhai.rocketmq.mysql.productor;

public interface Producer {

    default void start() throws Exception {}
    boolean push(String json);
    default void stop() {}
}
