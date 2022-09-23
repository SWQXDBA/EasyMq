package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.*
import kotlin.properties.Delegates

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
            //除了数据本身外其他信息所占空间
            val META_SIZE:Long = 40

            //delete标志位 1代表删除
            val DELETE_MARK = 0

            //数据大小
            val DATA_SIZE = 1

            //指针
            val POINTER = 9
//
//            //后继指针
//            val NEXT_POINTER = 17

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

    inner class IndexArray(val position: Long, val cap: Long) {
        operator fun get(index: Long): DoublePointer {
            return DoublePointer(position + INDEX_ARRAY.ARRAY_START + index * LongBytesSize)
        }
    }


    inner class DataBlock private constructor() {
         var  position by Delegates.notNull<Long>()

        /**
         * 创建一个已有的节点映射
         */
        constructor(position: Long) :this(){
            this.position = position
        }

        /**
         * 创建一个不存在的节点，为其分配空间
         */
        constructor(key: K, value: V) : this() {
            val keyBytes = keySerializer.toBytes(key)
            val valueBytes = valueSerializer.toBytes(value)
            this.position = alloc(keyBytes.size+valueBytes.size+DATA_AREA.META_SIZE)
            this.delete = false
            keySize = keyBytes.size


            valueSize = valueBytes.size
            hashCode = key.hashCode()

            fileMapper.position(keyStart).writeBytes(keyBytes)
            fileMapper.position(valueStart).writeBytes(valueBytes)


        }

        //根据指针位置 推算出数据块的起始位置
        constructor(pointer: DoublePointer) : this(pointer.position - DATA_AREA.POINTER)

        var delete: Boolean
            get() = fileMapper.position(position + DATA_AREA.DELETE_MARK).readByte() != 0.toByte()
            set(value) {
                val v: Byte = if (value) 1 else 0
                fileMapper.position(position + DATA_AREA.DELETE_MARK).writeByte(v)
            }

        val pointer: DoublePointer = DoublePointer(position + DATA_AREA.POINTER)

        var dataSize: Long
            get() = fileMapper.position(position + DATA_AREA.DATA_SIZE).readLong()
            private set(value) {
                fileMapper.position(position + DATA_AREA.DATA_SIZE).writeLong(value)
            }

        var keySize: Int
            get() = fileMapper.position(position + DATA_AREA.KEY_SIZE).readInt()
            private set(value) {
                fileMapper.position(position + DATA_AREA.KEY_SIZE).writeInt(value)
            }

        var valueSize: Int
            get() = fileMapper.position(position + DATA_AREA.VALUE_SIZE).readInt()
            private set(value) {
                fileMapper.position(position + DATA_AREA.VALUE_SIZE).writeInt(value)
            }
        var hashCode: Int
            get() = fileMapper.position(position + DATA_AREA.KEY_HASHCODE).readInt()
            private set(value) {
                fileMapper.position(position + DATA_AREA.KEY_HASHCODE).writeInt(value)
            }


        private val keyStart: Long
            get() = position + DATA_AREA.KEY_START

        private val valueStart: Long
            get() = keyStart + keySize

        val key: K
            get() = keySerializer.fromBytes(fileMapper.byteArrayAt(keyStart, keySize))

        val value: V
            get() {
                return valueSerializer.fromBytes(fileMapper.byteArrayAt(valueStart, valueSize))
            }
    }


    /**
     * 双向指针
     */
    inner class DoublePointer(val position: Long) {


        var prev: DoublePointer
            get() = DoublePointer(prevValue)
            set(value) {
                prevValue = value.position
            }
        var next: DoublePointer
            get() = DoublePointer(nextValue)
            set(value) {
                nextValue = value.position
            }

        var prevValue: Long
            get() = fileMapper.position(position + IndexPointer.PREV_POINTER).readLong()
            set(value) {
                fileMapper.position(position + IndexPointer.PREV_POINTER).writeLong(value)
            }

        var nextValue: Long
            get() = fileMapper.position(position + IndexPointer.NEXT_POINTER).readLong()
            set(value) {
                fileMapper.position(position + IndexPointer.NEXT_POINTER).writeLong(value)
            }
        val hasPrev: Boolean = prevValue == NULL_POINTER
        val hasNext: Boolean = nextValue == NULL_POINTER
        val isNull: Boolean = position == NULL_POINTER

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
        cap = newCap
        val dif = newCap - oldCap
        while (usageFileSize + dif > fileSize) {
            resizeFile()
        }

        val newPosition = alloc(newCap)
        indexArrayPosition = newPosition

        val newArray = IndexArray(newPosition, newCap)
        val oldArray = IndexArray(oldPosition, oldCap)
        for (i in 0 until oldCap) {
            var next = oldArray[i].next
            while (!next.isNull) {
                val current = next;
                next = current.next
                current.nextValue = NULL_POINTER
                current.prevValue = NULL_POINTER
                insert(DataBlock(current), newArray)
            }
        }
    }

    private fun insert(dataBlock: DataBlock, indexArray: IndexArray) {
        val hashIndex = hash(dataBlock.hashCode, indexArray.cap)
        val indexPointerNode = indexArray[hashIndex]

        if (!indexPointerNode.hasNext) {
            indexPointerNode.next = dataBlock.pointer
            dataBlock.pointer.prev = indexPointerNode
        } else {
            var lastPointer = indexPointerNode.next
            var next = lastPointer.next
            while (!next.isNull) {
                lastPointer = next
                next = lastPointer.next

                //同key 进行替换
                val currentData = DataBlock(lastPointer)
                if(currentData.hashCode==dataBlock.hashCode&& currentData.key==dataBlock.key){

                }
            }
            lastPointer.next = dataBlock.pointer
            dataBlock.pointer.prev = lastPointer
        }
    }

    private fun hash(hashCode: Int, cap: Long): Long {
        return hashCode % cap
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

        val dataBlock = DataBlock(key, value)
        insert(dataBlock,IndexArray(indexArrayPosition,cap))
        return value
    }

    override fun putAll(from: Map<out K, V>) {
        TODO("Not yet implemented")
    }

    override fun remove(key: K): V? {
        TODO("Not yet implemented")
    }


}