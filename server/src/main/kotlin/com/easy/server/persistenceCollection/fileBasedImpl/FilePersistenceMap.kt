package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.*

class FilePersistenceMap<K, V>(
    filePath: String,
    val keySerializer: Serializer<K>,
    val valueSerializer: Serializer<V>,
    autoForceMills: Long = 10,
    forcePerOption: Boolean = false,
    val initCap: Long = 16
) : PersistenceMap<K, V>, FilePersistenceCollection(filePath, autoForceMills, forcePerOption) {


    companion object {
        //文件类型标记
        val TYPE_MARK = 0

        //记录索引数组的容量
        val CAP = 8


        //记录元素数量
        val SIZE = 16

        //记录索引数组的位置
        val INDEX_ARRAY_Position = 24

        //记录文件已使用的字节大小
        val USAGE = 32

        //保留位
        val RESERVED = 40

        //数据区起始位置
        val DATA_AREA_START = 56


        object DATA_AREA {
            //delete标志位 1代表删除
            val DELETE_MARK = 0

            //数据大小
            val DATA_SIZE = 1

            //前驱指针
            val PREV_POINTER = 9

            //后继指针
            val NEXT_POINTER = 17

            //key的hashcode
            val KEY_HASHCODE = 25

            //key的大小
            val KEY_SIZE = 33

            //value的大小
            val VALUE_SIZE = 37

            //key的开始
            val KEY_START = 41
            //value的开始等于KEY_START+KEY_SIZE的值
        }

        object INDEX_ARRAY {
            //delete标志位 扩容的时候旧的索引数组会被删除
            val DELETE_MARK = 0

            //数据大小
            val SIZE = 1

            //索引指针数组的起始位置
            val ARRAY_START = 9
        }

        object IndexPointer {

            //所占大小为16字节
            val POINTER_LENGTH = LongBytesSize

            //前指针 索引指针没有前驱节点 所以前指针必须为-1 代表寻找结束
            val PREV_POINTER = 0

            //后指针
            val NEXT_POINTER = 8
        }

        //代表空指针
        val NULL_POINTER: Long = 0;
    }

    inner class IndexPointerNode(val index: Long) {

        var next: Long
            get() = fileMapper
                .position(INDEX_ARRAY_Position + INDEX_ARRAY.ARRAY_START + index * LongBytesSize)
                .readLong()
            set(value) {
                fileMapper
                    .position(INDEX_ARRAY_Position + INDEX_ARRAY.ARRAY_START + index * LongBytesSize)
                    .writeLong(value)
            }
    }

    inner class DataBlock(val position: Long) {
        var delete: Boolean
            get() = fileMapper.position(position + DATA_AREA.DELETE_MARK).readByte() != 0.toByte()
            set(value) {
                val v: Byte = if (value) 1 else 0
                fileMapper.position(position + DATA_AREA.DELETE_MARK).writeByte(v)
            }

        val dataSize: Long
            get() = fileMapper.position(position + DATA_AREA.DATA_SIZE).readLong()

        val keySize: Int
            get() = fileMapper.position(position + DATA_AREA.KEY_SIZE).readInt()
        val valueSize: Int
            get() = fileMapper.position(position + DATA_AREA.VALUE_SIZE).readInt()
        val hashCode: Long
            get() = fileMapper.position(position + DATA_AREA.KEY_HASHCODE).readLong()

        var prev: Long
            get() = fileMapper
                .position(position + DATA_AREA.PREV_POINTER)
                .readLong()
            set(value) {
                fileMapper
                    .position(position + DATA_AREA.PREV_POINTER)
                    .writeLong(value)
            }

        var next: Long
            get() = fileMapper
                .position(position + DATA_AREA.NEXT_POINTER)
                .readLong()
            set(value) {
                fileMapper
                    .position(position + DATA_AREA.NEXT_POINTER)
                    .writeLong(value)
            }
        private val keyStart: Long
            get() = fileMapper.position(position + DATA_AREA.KEY_START).readLong()
        private val valueStart: Long
            get() = keyStart+keySize
        val key: K
            get() = keySerializer.fromBytes(fileMapper.byteArrayAt(keyStart, keySize))
        val value: V
            get() {
                return valueSerializer.fromBytes(fileMapper.byteArrayAt(valueStart, valueSize))
            }
    }

    init {

        val type = fileMapper.position(TYPE_MARK).readLong()
        if (type == FileType.NEW_FILE) {
            init()
        } else if (type != FileType.MutableMap) {
            throw Exception("文件类型不匹配！需要 ${FileType.MutableMap} 但是检测为$type")
        }
    }

    var usageFileSize: Long
        get() = fileMapper.position(USAGE).readLong()
        set(value) {
            fileMapper.position(USAGE).writeLong(value)
        }

    var cap: Long
        get() = fileMapper.position(CAP).readLong()
        set(value) {
            fileMapper.position(CAP).writeLong(value)
        }
    var indexArrayPosition: Long
        get() = fileMapper.position(INDEX_ARRAY_Position).readLong()
        set(value) {
            fileMapper.position(INDEX_ARRAY_Position).writeLong(value)
        }

    /**
     * 分配新的空间，返回空间的起始位置
     */
    fun alloc(size: Long): Long {
        val start = usageFileSize
        while (start + size > fileSize) {
            resizeFile()
        }
        usageFileSize = start + size
        return start
    }

    fun init() {
        //设置文件类型
        fileMapper.position(TYPE_MARK).writeLong(FileType.MutableMap)
        cap = initCap
        //分配索引数组
        val indexArraySize = IndexPointer.POINTER_LENGTH * cap
        val alloc = alloc(indexArraySize)
        indexArrayPosition = alloc
    }

    fun expansion(newCap: Long) {
        val oldPosition = indexArrayPosition
        val oldCap = cap
        val dif = newCap - oldCap
        while (usageFileSize + dif > fileSize) {
            resizeFile()
        }

        val newPosition = alloc(newCap)
        for (i in 0 until oldCap) {
            var next = IndexPointerNode(i).next
            while (next != NULL_POINTER) {

            }

        }


    }

    override val size: Int
        get() = fileMapper.position(SIZE).readInt()
    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = TODO("Not yet implemented")
    override val keys: MutableSet<K>
        get() = TODO("Not yet implemented")
    override val values: MutableCollection<V>
        get() = TODO("Not yet implemented")


    override fun containsKey(key: K): Boolean {
        TODO("Not yet implemented")
    }

    override fun containsValue(value: V): Boolean {
        TODO("Not yet implemented")
    }

    override fun get(key: K): V? {
        TODO("Not yet implemented")
    }

    override fun isEmpty(): Boolean {
        TODO("Not yet implemented")
    }

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun put(key: K, value: V): V? {
        TODO("Not yet implemented")
    }

    override fun putAll(from: Map<out K, V>) {
        TODO("Not yet implemented")
    }

    override fun remove(key: K): V? {
        TODO("Not yet implemented")
    }


}