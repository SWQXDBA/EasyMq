package com.easy.core.message;

import com.easy.core.entity.MessageId;

import java.io.Serializable;

/**
 * 储存在硬盘中的消息。
 */
public class PersistentMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    MessageId id;
    String topic;
    byte[] data;
}
