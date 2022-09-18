package com.easy.server.dao

import com.easy.core.entity.MessageId
import com.easy.core.message.TransmissionMessage

interface PersistenceProvider {
    /**
     * 保存消息
     */
   suspend fun save(transmissionMessage : TransmissionMessage)

    /**
     * 删除消息
     */
    fun remove(messageId: MessageId)

    /**
     * 持久化自身元数据 定时调用
     */
    fun persistMeta()

    /**
     * 加载自身元数据 在服务端启动时调用
     */
    fun loadMeta()
}