package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch


abstract class AbstractFilePersistenceCollection(
    filePath: String,
    var initCap: Int = 16,
     fileMapperType: FileMapperType
) {
    var fileMapper: FileMapper
    var autoForceMills: Long = 10

    var fileSize: Long
        get() = fileMapper.fileSize
        set(value) {
            fileMapper.fileSize = value
        }

    val initFileSize = 1024 * 1024L

    init {
         //fileMapper = MemoryMapMapper(filePath, initFileSize)
        fileMapper =  when(fileMapperType){
            FileMapperType.MemoryMapMapper-> MemoryMapMapper(filePath, initFileSize)
            FileMapperType.RandomAccessFileMapper ->  RandomAccessFileMapper(filePath, initFileSize)
            FileMapperType.MergedMemoryMapMapper  ->  MergedMemoryMapMapper(filePath, initFileSize)
        }





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
    /**
     * 分配新的空间，返回空间的起始位置
     */
    fun alloc(size: Long): Long {



        val start = usageFileSize

        while (start + size >= fileSize) {

            resizeFile()
        }

        usageFileSize = start + size


        return start
    }


    abstract var usageFileSize: Long

    abstract var cap: Int

    abstract var indexArrayPosition: Long


    abstract val indexArray: IndexArray<out Any>

    abstract inner class IndexArray<T>(val position:Long,val cap:Int){
        abstract operator fun get(index:Int):T
    }
    abstract fun expansion(newCap: Int)
    abstract fun computeIndexArraySize(cap: Int): Long
}