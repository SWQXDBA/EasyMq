package com.easy.server.core.entity;

import com.easy.core.Client;

import java.util.Set;

public class Producer extends Client {
    /**
     * 用于保证消息不会被重复接收
     */
    Set<Long> messageIds;
}
