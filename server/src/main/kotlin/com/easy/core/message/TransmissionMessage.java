package com.easy.core.message;

import com.easy.core.entity.MessageId;

/**
 * 在消息队列内存中储存的消息 服务器重启后会被清除 需要重新从PersistentMessage以及MessageMetaInfo进行创建。
 *
 * @author SWQXDBA
 */
public class TransmissionMessage {
    /**
     * Topic下的全局id 只会生成一次
     */
    public MessageId id;

    /**
     * 真正要传递的数据
     */
    public byte[] data;


}