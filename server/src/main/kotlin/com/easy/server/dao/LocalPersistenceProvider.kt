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
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.function.Consumer
import kotlin.coroutines.suspendCoroutine
import kotlin.io.path.pathString

@Service
class LocalPersistenceProvider : PersistenceProvider {
    final var fileChannel: FileChannel;
init {
    val path = Path.of(File("").absolutePath,"data.txt")

    fileChannel =
        FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

}
    val objectMapper = ObjectMapper()

    override suspend fun save(transmissionMessage : TransmissionMessage)  = suspendCoroutine<Unit>{

        GlobalScope.launch(Dispatchers.IO) {
            val bytes = objectMapper.writeValueAsBytes(transmissionMessage)
            fileChannel.write(ByteBuffer.wrap(bytes)
            )
            it.resumeWith(Result.success(Unit))
        }
    }




}