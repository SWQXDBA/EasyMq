package com.easy.core.enums;

//消息类型
public enum ClientMessageType {

    /**
     * 仅仅是一个心跳包 意味着没有携带任何其他消息
     */
    ALIVE,

    /**
     * 请求连接到服务器
     */
    CONNECT,

    /**
     * 请求消费一个消息 同时也提供心跳刷新的功能
     */
    MESSAGE
}
