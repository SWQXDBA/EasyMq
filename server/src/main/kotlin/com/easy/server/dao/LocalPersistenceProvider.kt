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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.suspendCoroutine
import kotlin.io.path.pathString

@Service
class LocalPersistenceProvider : PersistenceProvider {

    var fileChannel: FileChannel? = null

    var currentFileName: String? = null

    final val basePath = ""

    final val MAX_STORED_MESSAGE_PER_FILE = 100000L

    /**
     * 用于记录这个messageid的是存在哪个文件
     */
    val messageFileMap = ConcurrentHashMap<MessageId, String>()

    /**
     * 用于记录文件中保存的，未响应的文件数量
     */
    final val fileUnconfirmedMessageSize = ConcurrentHashMap<String, Long>()

    /**
     * 用于记录文件中保存的消息数量
     */
    final val fileStoredMessageSize = ConcurrentHashMap<String, Long>()

    final val fileIndex = AtomicInteger()




    val objectMapper = ObjectMapper()

    @Synchronized
    private fun switchFileAndWrite(message:ByteBuffer): Pair<FileChannel, String> {

        if (
        //已经初始化
            currentFileName != null
            && fileStoredMessageSize.containsKey(currentFileName)
            //并且文件没满
            && fileStoredMessageSize[currentFileName]!! <= MAX_STORED_MESSAGE_PER_FILE
        ) {
            fileChannel!!.write(message)
            return Pair(fileChannel!!, currentFileName!!)
        }

        val path = Path.of(File(basePath).absolutePath, "data${fileIndex.getAndIncrement()}.txt")
        currentFileName = path.pathString

        fileChannel?.close()
        fileChannel =
            FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        fileUnconfirmedMessageSize[currentFileName!!] = 0
        fileStoredMessageSize[currentFileName!!] = 0

        println("切换了$currentFileName")
        fileChannel!!.write(message)
        return Pair(fileChannel!!, currentFileName!!)

    }

    override suspend fun save(transmissionMessage: TransmissionMessage) = suspendCoroutine<Unit> {

        GlobalScope.launch(Dispatchers.IO) {
            val bytes = objectMapper.writeValueAsBytes(transmissionMessage)
            val channelAndName: Pair<FileChannel, String> = switchFileAndWrite(ByteBuffer.wrap(bytes))

            val channelFileName = channelAndName.second

            messageFileMap[transmissionMessage.id] = channelFileName
            fileUnconfirmedMessageSize.computeIfPresent(channelFileName) { _, value -> value + 1 }
            fileStoredMessageSize.computeIfPresent(channelFileName) { _, value -> value + 1 }

            it.resumeWith(Result.success(Unit))
        }
    }

    override fun remove(messageId: MessageId) {
        val fileName = messageFileMap[messageId] ?: return
        messageFileMap.remove(messageId)
        val count = fileUnconfirmedMessageSize.computeIfPresent(fileName) { _, value -> value - 1 }
        val fileMessageCount = fileStoredMessageSize.getOrDefault(fileName, MAX_STORED_MESSAGE_PER_FILE)
        if (fileMessageCount >= MAX_STORED_MESSAGE_PER_FILE) {
            count?.let {
                if (it <= 0L) {
                    fileUnconfirmedMessageSize.remove(fileName)
                    fileStoredMessageSize.remove(fileName)
                    try {
                        println("删除了$fileName")
                        Files.delete(Path.of(fileName))
                    } catch (e: Exception) {
                        e.printStackTrace()
                    }
                }
            }
        }

    }

    override fun persistMeta() {
        TODO("Not yet implemented")
    }

    override fun loadMeta() {
        TODO("Not yet implemented")
    }


}