package com.easy.server.dao

import com.easy.core.entity.MessageId

import com.easy.core.message.TransmissionMessage
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.springframework.stereotype.Service
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import kotlin.coroutines.suspendCoroutine
import kotlin.io.path.pathString

@Service
class LocalPersistenceProvider : PersistenceProvider {
    final var fileChannel: FileChannel;
    final val bastPath = ""

    /**
     * 用于记录这个messageid的是存在哪个文件
     */
    val messageFileMap = ConcurrentHashMap<MessageId, String>()

    /**
     * 用于记录文件中保存的，未响应的文件数量，如果归零了，则可以删除这个文件。
     */
    val fileUnconfirmedMessageSize = ConcurrentHashMap<String, Long>()

    init {
        val path = Path.of(File(bastPath).absolutePath, "data.txt")

        fileChannel =
            FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

    }

    val objectMapper = ObjectMapper()

    override suspend fun save(transmissionMessage: TransmissionMessage) = suspendCoroutine<Unit> {

        GlobalScope.launch(Dispatchers.IO) {
            val bytes = objectMapper.writeValueAsBytes(transmissionMessage)
            fileChannel.write(
                ByteBuffer.wrap(bytes)
            )
            it.resumeWith(Result.success(Unit))
        }
    }

    suspend fun confirm(messageId: MessageId) {
        val fileName = messageFileMap[messageId] ?: return
        val count = fileUnconfirmedMessageSize.computeIfPresent(fileName) { _, value -> value - 1 }
        count?.let {
            if (it == 0L) {
                fileUnconfirmedMessageSize.remove(fileName)
                try{
                    Files.delete(Path.of(fileName))
                }catch (e:Exception){
                    e.printStackTrace()
                }
            }
        }
    }


}