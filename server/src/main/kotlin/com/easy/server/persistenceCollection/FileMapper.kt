package com.easy.server.persistenceCollection

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.exists

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
    fun writeBytes(value:ByteArray)
    fun writeByte(value:Byte)
    fun writeInt(value: Int)
    fun writeLong(value: Long)
    fun writeDouble(value: Double)


    fun readInt(): Int
    fun readLong(): Long
    fun readDouble(): Double
    fun readByte():Byte
    fun readBytes(value:ByteArray)
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
        }else{
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
class RandomAccessFileMapper(  filePath: String,
                               initFileSize: Long
):FileMapper{
    var randomAccessFile:RandomAccessFile
    override var fileSize: Long
        get() = randomAccessFile.length()
        set(value) {

            randomAccessFile.setLength(value)
        }

    init {
        if(Files.exists(Path.of(filePath))){
            randomAccessFile = RandomAccessFile(filePath,"rw")
        }else{
            randomAccessFile = RandomAccessFile(filePath,"rw")
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
      return  randomAccessFile.readInt()
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