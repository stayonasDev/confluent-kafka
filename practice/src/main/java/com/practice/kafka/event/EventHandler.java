package com.practice.kafka.event;

import org.apache.kafka.common.protocol.Message;

import java.util.concurrent.ExecutionException;

public interface EventHandler {
    void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException;
}
