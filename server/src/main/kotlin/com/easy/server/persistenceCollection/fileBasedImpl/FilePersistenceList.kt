package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.PersistenceList
import com.easy.server.persistenceCollection.Serializer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.exists

class FilePersistenceList<E>(
    val filePath: String,
    val serializer: Serializer<E>
) : PersistenceList<E> {
    //数据块原信息
    data class BlockMetaData(
        val delete: Boolean,
        val dataHashCode: Int,
        val dataSize: Int,
        //数据在文件中的偏移量
        val dataPosition: Int
    )


    var fileMapper: MappedByteBuffer

    /**
     * 表示4个字节
     */
    val intByteSize = 4

    //1mb初始大小
    var fileSize = 1024 * 1024L

    override val size: Int
        get() = fileMapper.position(SIZE_POSITION).asIntBuffer().get()


    private val indexCap: Int
        get() = fileMapper.position(INDEX_CAP_POSITION).asIntBuffer().get()


    companion object {
        //数据个数地址
        val SIZE_POSITION = 4

        //索引容量地址
        val INDEX_CAP_POSITION = 0

        //索引起始偏移量
        val OFFSET_INDEX_POSITION = 5

        //删除标志位在数据块中的偏移量
        val DELETE_OFFSET = 0

        //hashcode在数据块中的偏移量
        val DATA_HASHCODE_OFFSET = 4

        //hashcode在数据块中的偏移量
        val DATA_SIZE_OFFSET = 8

        //数据本体数据块中的偏移量
        val DATA_OFFSET = 9
    }


    /**
     * 数据区起始偏移量
     */
    val dataAreaPosition: Int
        get() = 4 * (indexCap + 2)

    init {
        val path = Path.of(filePath)
        if (path.exists()) {
            fileSize = Files.size(path)

        }
        fileMapper =
            FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                .map(FileChannel.MapMode.READ_WRITE, 0, fileSize)


    }

    /**
     * 根据索引(数据区偏移量)得到数据元信息
     */
    private fun getBlockMetaByOffsetIndex(offsetIndex: Int): BlockMetaData {
        val delete = fileMapper.position(offsetIndex + dataAreaPosition + DELETE_OFFSET).get() == 1.toByte()
        val dataHashcode =
            fileMapper.position(offsetIndex + dataAreaPosition + DATA_HASHCODE_OFFSET).asIntBuffer().get()
        val dataSize = fileMapper.position(offsetIndex + dataAreaPosition + DATA_SIZE_OFFSET).asIntBuffer().get()
        return BlockMetaData(delete, dataHashcode, dataSize, offsetIndex + DATA_OFFSET)
    }


    /**
     * 根据index找到偏移索引,得出数据区偏移量
     * index: 第几个元素
     */
    private fun getOffsetIndexByIndex(index: Int): Int {
        return fileMapper.position(OFFSET_INDEX_POSITION + index * intByteSize).asIntBuffer().get()
    }

    private fun getByteArray(position: Int, length: Int): ByteArray {
        val arr = ByteArray(length)
        fileMapper.position(position)
        fileMapper.get(arr)
        return arr
    }

    override fun contains(element: E): Boolean {

        return indexOf(element)!=-1

    }

    override fun containsAll(elements: Collection<E>): Boolean {
       var match = true
        for (element in elements) {
            match = match and contains(element)
        }
        return match
    }

    override fun get(index: Int): E {
        val offset = getOffsetIndexByIndex(index)
        val meta = getBlockMetaByOffsetIndex(offset)
        val byteArray = getByteArray(meta.dataPosition, meta.dataSize)
        return serializer.fromBytes(byteArray)
    }

    override fun indexOf(element: E): Int {
        for (i in 0..size) {
            //根据索引 找到数据块的数据区偏移量
            val offset = getOffsetIndexByIndex(i)
            //读前9个byte 获取信息
            val meta = getBlockMetaByOffsetIndex(offset)
            //比较hashcode
            if (element.hashCode() == meta.dataHashCode) {

                val bytes = serializer.toBytes(element)
                //比较字节数
                if (bytes.size == meta.dataSize) {
                    val fromBytes = serializer.fromBytes(getByteArray(meta.dataPosition, meta.dataSize))
                    if (element == fromBytes) {
                        return i
                    }
                }
            }
        }
        return -1
    }

    override fun isEmpty(): Boolean {
        return size == 0
    }


    override fun add(element: E): Boolean {
        TODO("Not yet implemented")
    }

    override fun add(index: Int, element: E) {
        TODO("Not yet implemented")
    }

    override fun addAll(index: Int, elements: Collection<E>): Boolean {
        TODO("Not yet implemented")
    }

    override fun addAll(elements: Collection<E>): Boolean {
        TODO("Not yet implemented")
    }

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun remove(element: E): Boolean {
        TODO("Not yet implemented")
    }

    override fun removeAll(elements: Collection<E>): Boolean {
        TODO("Not yet implemented")
    }

    override fun removeAt(index: Int): E {
        TODO("Not yet implemented")
    }

    override fun retainAll(elements: Collection<E>): Boolean {
        TODO("Not yet implemented")
    }

    override fun set(index: Int, element: E): E {
        TODO("Not yet implemented")
    }


    override fun iterator(): MutableIterator<E> {
        TODO("Not yet implemented")
    }

    override fun lastIndexOf(element: E): Int {
        TODO("Not yet implemented")
    }

    override fun listIterator(): MutableListIterator<E> {
        TODO("Not yet implemented")
    }

    override fun listIterator(index: Int): MutableListIterator<E> {
        TODO("Not yet implemented")
    }

    override fun subList(fromIndex: Int, toIndex: Int): MutableList<E> {
        TODO("Not yet implemented")
    }
}