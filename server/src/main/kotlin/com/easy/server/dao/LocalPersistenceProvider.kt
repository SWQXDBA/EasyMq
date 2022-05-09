package com.easy.server.dao

import com.easy.core.entity.MessageId
import com.easy.core.message.PersistentMessage
import com.easy.core.message.TransmissionMessage
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.function.Consumer
import kotlin.coroutines.suspendCoroutine

@Service
class LocalPersistenceProvider : PersistenceProvider {
    final var fileChannel: FileChannel =
        FileChannel.open(Path.of(""), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

    val objectMapper = ObjectMapper()

    override suspend fun save(topicName: String, messageId: MessageId,data:ByteArray)  = suspendCoroutine<TransmissionMessage>{
        val persistentMessage = PersistentMessage(messageId,topicName, data)
        GlobalScope.launch(Dispatchers.IO) {
            val bytes = objectMapper.writeValueAsBytes(persistentMessage)
            fileChannel.write(ByteBuffer.wrap(bytes)
            )
            val transmissionMessage = TransmissionMessage()
            transmissionMessage.setId(messageId)
            transmissionMessage.setData(data)
            it.resumeWith(Result.success(transmissionMessage))
        }
    }




}