package com.easy.core.message;

import com.easy.core.entity.MessageId;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 在消息队列内存中储存的消息 服务器重启后会被清除 需要重新从PersistentMessage以及MessageMetaInfo进行创建。
 *
 * @author SWQXDBA
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
public class TransmissionMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Topic下的全局id 只会生成一次
     */
    public MessageId id;

    /**
     * 真正要传递的数据
     */
    public byte[] data;

    /**
     * 发送时 data的类型
     */
    Class<?> dataClass;

    String topicName;

    /**
     * 需要回调
     */
    boolean needCallBack;

    public TransmissionMessage(MessageId id, byte[] data, Class<?> dataClass,String topicName, boolean callBack) {
        this.id = id;
        this.data = data;
        this.dataClass = dataClass;
        this.topicName = topicName;
        this.needCallBack = callBack;
    }
}
