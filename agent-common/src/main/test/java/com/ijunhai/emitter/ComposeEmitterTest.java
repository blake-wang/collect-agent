package com.ijunhai.emitter;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ComposeEmitterTest {

    @Test
    public void testComposeEmitter() {
        String a = "logging, rocketmq";
        ComposeEmitter composeEmitter = new ComposeEmitter(a);
        List<Emitter> emitters = composeEmitter.getEmitters();
        Assert.assertEquals(emitters.size(), 2);
    }
}
