package com.easy.core.message;

import com.easy.core.entity.MessageId;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * 储存在硬盘中的消息。
 */
@Getter
@Setter
@NoArgsConstructor
public class PersistentMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    MessageId id;
    String topicName;
    byte[] data;


    public PersistentMessage(MessageId id, String topicName, byte[] data) {
        this.id = id;
        this.topicName = topicName;
        this.data = data;
    }
}
