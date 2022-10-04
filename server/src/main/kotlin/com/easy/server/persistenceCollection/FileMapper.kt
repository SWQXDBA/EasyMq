package com.easy.server.persistenceCollection

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.exists
import kotlin.math.min

interface FileMapper {
    /**
     * 代表文件的大小，需要在set中实现修改功能
     */
    var fileSize: Long

    /**
     * 设置写入位置
     */
    fun position(position: Long): FileMapper

    /**
     * 设置写入位置
     */
    fun position(position: Int): FileMapper

    /**
     * 把从 position开始length个字节 移动step个字节
     * position:起始位置 step: 移动多少个字节 正数代表后移 负数代表前移
     */
    fun moveBytes(position: Long, length: Int, step: Long) {
        if (length == 0 || step == 0L) {
            return
        }
        val temp = ByteArray(length)
        position(position).readBytes(temp)
        position(position + step).writeBytes(temp)

    }

    fun writeBytes(value: ByteArray)
    fun writeByte(value: Byte)
    fun writeInt(value: Int)
    fun writeLong(value: Long)
    fun writeDouble(value: Double)


    fun readInt(): Int
    fun readLong(): Long
    fun readDouble(): Double
    fun readByte(): Byte
    fun readBytes(value: ByteArray)
    fun force()

    fun byteArrayAt(position: Long, length: Int): ByteArray {
        val arr = ByteArray(length)
        position(position)
        readBytes(arr)
        return arr
    }
}

/**
 * 基于MappedByteBuffer的实现 最大为Int.MAV_VALUE 且position最大只支持Int.MAV_VALUE
 */
class MemoryMapMapper(
    private var filePath: String,
    initFileSize: Long
) : FileMapper {

    private lateinit var fileMapper: MappedByteBuffer


    override var fileSize: Long = 0
        set(value) {
            val path = Path.of(filePath)
            fileMapper =
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, value)
            field = value
        }

    init {
        val path = Path.of(filePath)
        if (path.exists()) {
            this.fileSize = Files.size(path)
        } else {
            this.fileSize = initFileSize
        }
    }


    /**
     * 此实现中，position只能为Int
     */
    override fun position(position: Long): FileMapper {
        fileMapper.position(position.toInt())
        return this
    }

    override fun position(position: Int): FileMapper {
        return position(position.toLong())
    }

    override fun writeBytes(value: ByteArray) {
        fileMapper.put(value)
    }

    override fun writeByte(value: Byte) {
        fileMapper.put(value)
    }

    override fun writeInt(value: Int) {
        fileMapper.asIntBuffer().put(value)
    }

    override fun writeLong(value: Long) {
        fileMapper.asLongBuffer().put(value)
    }

    override fun writeDouble(value: Double) {
        fileMapper.asDoubleBuffer().put(value)
    }

    override fun readInt(): Int {
        return fileMapper.asIntBuffer().get()
    }

    override fun readLong(): Long {
        return fileMapper.asLongBuffer().get()
    }

    override fun readDouble(): Double {
        return fileMapper.asDoubleBuffer().get()
    }

    override fun readByte(): Byte {
        return fileMapper.get()
    }

    override fun readBytes(value: ByteArray) {
        fileMapper.get(value)
    }

    override fun force() {
        fileMapper.force()
    }

}

/**
 * 基于RandomAccessFile的实现 支持Long.MAX_VALUE
 */
class RandomAccessFileMapper(
    filePath: String,
    initFileSize: Long
) : FileMapper {
    var randomAccessFile: RandomAccessFile
    override var fileSize: Long
        get() = randomAccessFile.length()
        set(value) {

            randomAccessFile.setLength(value)
        }

    init {
        if (Files.exists(Path.of(filePath))) {
            randomAccessFile = RandomAccessFile(filePath, "rw")
        } else {
            randomAccessFile = RandomAccessFile(filePath, "rw")
            fileSize = initFileSize
        }
    }

    override fun position(position: Long): FileMapper {
        randomAccessFile.seek(position)
        return this
    }

    override fun position(position: Int): FileMapper {
        return position(position.toLong())
    }

    override fun writeBytes(value: ByteArray) {
        randomAccessFile.write(value)
    }

    override fun writeByte(value: Byte) {
        randomAccessFile.writeByte(value.toInt())
    }

    override fun writeInt(value: Int) {
        randomAccessFile.writeInt(value)
    }

    override fun writeLong(value: Long) {
        randomAccessFile.writeLong(value)
    }

    override fun writeDouble(value: Double) {
        randomAccessFile.writeDouble(value)
    }

    override fun readInt(): Int {
        return randomAccessFile.readInt()
    }

    override fun readLong(): Long {
        return randomAccessFile.readLong()
    }

    override fun readDouble(): Double {
        return randomAccessFile.readDouble()
    }

    override fun readByte(): Byte {
        return randomAccessFile.readByte()
    }

    override fun readBytes(value: ByteArray) {
        randomAccessFile.read(value)
    }

    override fun force() {

    }

}

class MergedMemoryMapMapper(
    val filePath: String,
    initFileSize: Long
) : FileMapper {




    val sizePerMap: Long = Int.MAX_VALUE.toLong()
    private lateinit var selectedMapper: MappedByteBuffer
    private var selectedMapperIndex: Int = 0
    val mapArray = mutableListOf<MappedByteBuffer>()

    private val buffer32:ByteBuffer = ByteBuffer.allocate(4)
    private val buffer64:ByteBuffer = ByteBuffer.allocate(8)

    override var fileSize: Long = 0
        set(value) {
            resizeMaps(value)
            field = value
        }

    init {
        fileSize = initFileSize
    }

    private fun getMapperFileName(index: Int): String {
        return "$filePath - $index"
    }

    private fun mapper(index: Int, size: Long): MappedByteBuffer {
        return FileChannel.open(
            Path.of(getMapperFileName(index)),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        )
            .map(FileChannel.MapMode.READ_WRITE, 0, size)
    }

    private fun getLastMapperIndex(position: Long): Int {
        return (position / sizePerMap).toInt()
    }

    private fun getPositionInLastMapper(position: Long): Long {
        return position % sizePerMap;
    }

    private fun resizeMaps(newSize: Long) {
        val oldSize = fileSize
        if (newSize == oldSize) {
            return
        }
        if (mapArray.isEmpty()) {
            if (newSize >= sizePerMap) {
                mapArray.add(mapper(0, sizePerMap))
            } else {
                mapArray.add(mapper(0, newSize))
            }
        }
        val newMapperIndex = getLastMapperIndex(newSize)
        val currentLastMapperIndex = mapArray.size - 1
        val positionInLastMapper = getPositionInLastMapper(newSize)
        if (newMapperIndex == currentLastMapperIndex) {
            mapArray[newMapperIndex].force()
            mapArray[newMapperIndex] = mapper(newMapperIndex, positionInLastMapper)
        } else if (newMapperIndex > currentLastMapperIndex) {
            if (mapArray[currentLastMapperIndex].capacity() < sizePerMap) {
                mapArray[currentLastMapperIndex] = mapper(currentLastMapperIndex, sizePerMap)
            }
            for (i in currentLastMapperIndex + 1 until newMapperIndex) {
                mapArray.add(mapper(i, sizePerMap))
            }
            mapArray.add(mapper(newMapperIndex, positionInLastMapper))
        } else { //lastMapperIndex<currentIndex
            for (i in currentLastMapperIndex downTo newMapperIndex + 1) {
                mapArray.removeAt(i)
                Files.deleteIfExists(Path.of(getMapperFileName(i)))
            }
            mapArray[newMapperIndex].force()
            mapArray[newMapperIndex] = mapper(newMapperIndex, positionInLastMapper)
        }
    }


    override fun position(position: Long): FileMapper {
        selectedMapperIndex = getLastMapperIndex(position)
        selectedMapper = mapArray[selectedMapperIndex]
        selectedMapper.position(getPositionInLastMapper(position).toInt())

        return this
    }

    override fun position(position: Int): FileMapper {
        return position(position.toLong())
    }


    override fun writeBytes(value: ByteArray) {
        optionBytes(value,false)
    }



    override fun writeByte(value: Byte) {
        selectedMapper.put(value)
    }

    override fun writeInt(value: Int) {
        buffer32.clear()
        buffer32.putInt(value)
        writeBytes(buffer32.array())
    }

    override fun writeLong(value: Long) {
        buffer64.clear()
        buffer64.putLong(value)
        writeBytes(buffer64.array())

    }

    override fun writeDouble(value: Double) {
        buffer64.clear()
        buffer64.putDouble(value)
        writeBytes(buffer64.array())
    }

    override fun readInt(): Int {

        readBytes(buffer32.array())
        buffer32.clear()
        return  buffer32.int
    }

    override fun readLong(): Long {
        readBytes(buffer64.array())
        buffer64.clear()
        return buffer64.long
    }

    override fun readDouble(): Double {
        readBytes(buffer64.array())
        buffer64.clear()
        return selectedMapper.double
    }

    override fun readByte(): Byte {
        return selectedMapper.get()
    }

    override fun readBytes(value: ByteArray) {
        optionBytes(value,true)
    }

    /**
     * 读取到数组中，或者把数组中的数据写入mapper，
     * readToArray == true则为读取到数组中
     * readToArray == false则为把数组中的数据写入mapper
     */
    private fun optionBytes(array: ByteArray, readToArray:Boolean){
        var mapper = selectedMapper
        var mapperIndex = selectedMapperIndex
        var position = mapper.position()
        if (position + array.size <= sizePerMap) {
            if(readToArray){
                mapper.get(array)
            }else{
                mapper.put(array)
            }

        } else {

            var offset = 0
            while (offset < array.size) {
                val temp = mapper.position()

                mapper.position(position)
                val bytesCanReadInThisMapper = mapper.capacity() - position
                val bytesToRead = min(bytesCanReadInThisMapper,array.size-offset)
                if(readToArray){
                    mapper.get(array, offset, bytesToRead)
                }else{
                    mapper.put(array, offset, bytesToRead)
                }


                offset += bytesToRead
                position = 0
                if (offset == array.size) {
                    return
                }

                mapper.position(temp)
                mapper = mapArray[++mapperIndex]
            }

        }
    }
    override fun force() {
        mapArray.forEach { it.force() }
    }
}