package com.easy.server.dao

import com.easy.core.entity.MessageId
import com.easy.core.message.TransmissionMessage
import java.util.function.Consumer

interface PersistenceProvider {
   suspend fun save(transmissionMessage : TransmissionMessage)
    fun confirm(messageId: MessageId)
}