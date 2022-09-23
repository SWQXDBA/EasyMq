package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.FileMapper
import com.easy.server.persistenceCollection.MemoryMapMapper
import com.easy.server.persistenceCollection.Serializer
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch


abstract class FilePersistenceCollection(
    filePath: String,
    val autoForceMills: Long = 10,
    val forcePerOption: Boolean = false
) {
    var fileMapper: FileMapper

    var fileSize: Long
        get() = fileMapper.fileSize
        set(value) {
            fileMapper.fileSize = value
        }

    val initFileSize = 1024 * 1024L

    init {
        fileMapper = MemoryMapMapper(filePath, initFileSize)
        // fileMapper = RandomAccessFileMapper(filePath,fileSize)
        GlobalScope.launch {
            while (true) {
                delay(autoForceMills)
                fileMapper.force()
            }
        }
    }

    fun resizeFile(magnification: Int = 2) {
        fileSize *= magnification
    }
}