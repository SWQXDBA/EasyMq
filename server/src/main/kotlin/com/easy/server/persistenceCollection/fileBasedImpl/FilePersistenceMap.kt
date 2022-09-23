package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.*
import javax.xml.crypto.Data
import kotlin.properties.Delegates

class FilePersistenceMap<K, V>(
    filePath: String,
    val keySerializer: Serializer<K>,
    val valueSerializer: Serializer<V>,
    autoForceMills: Long = 10,
    forcePerOption: Boolean = false,
    val initCap: Int = 16
) : PersistenceMap<K, V>, FilePersistenceCollection(filePath, autoForceMills, forcePerOption) {


    companion object {
        //文件类型标记
        val TYPE_MARK = 0

        //记录索引数组的容量
        val CAP = 8


        //记录元素数量
        val SIZE = 12

        //记录索引数组的位置
        val INDEX_ARRAY_Position = 16

        //记录文件已使用的字节大小
        val USAGE = 24

        //保留位
        val RESERVED = 32

        //数据区起始位置
        val DATA_AREA_START = 48


        object DATA_AREA {
            //除了数据本身外其他信息所占空间
            const val META_SIZE: Long = 40

            //delete标志位 1代表删除
            const val DELETE_MARK = 0

            //数据大小
            const val DATA_SIZE = 1

            //指针
            const val POINTER = 9
//
//            //后继指针
//            val NEXT_POINTER = 17

            //key的hashcode
            const val KEY_HASHCODE = 25

            //key的大小
            const val KEY_SIZE = 29

            //value的大小
            const val VALUE_SIZE = 33

            //key的开始
            const val KEY_START = 37
            //value的开始等于KEY_START+KEY_SIZE的值
        }

        object INDEX_ARRAY {
            //delete标志位 扩容的时候旧的索引数组会被删除
            val DELETE_MARK = 0

            //数据大小
            val DATA_SIZE = 1

            //索引指针数组的起始位置
            val ARRAY_START = 9
        }

        object IndexPointer {

            //所占大小为16字节
            val POINTER_LENGTH = LongBytesSize * 2

            //前指针 索引指针没有前驱节点 所以前指针必须为-1 代表寻找结束
            val PREV_POINTER = 0

            //后指针
            val NEXT_POINTER = 8
        }

        //代表空指针
        val NULL_POINTER: Long = 0;
    }

    inner class IndexArray(val position: Long, val cap: Int) {
        operator fun get(index: Int): DoublePointer {
            return DoublePointer(position + INDEX_ARRAY.ARRAY_START + index * LongBytesSize)
        }
    }


    inner class DataBlock private constructor() {
        var position by Delegates.notNull<Long>()

        /**
         * 创建一个已有的节点映射
         */
        constructor(position: Long) : this() {
            this.position = position
        }

        /**
         * 创建一个不存在的节点，为其分配空间
         */
        constructor(key: K, value: V) : this() {
            val keyBytes = keySerializer.toBytes(key)
            val valueBytes = valueSerializer.toBytes(value)

            this.position = alloc(keyBytes.size + valueBytes.size + DATA_AREA.META_SIZE)


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

        val pointer: DoublePointer by lazy { DoublePointer(position + DATA_AREA.POINTER) }

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
        val hasPrev: Boolean get() = prevValue != NULL_POINTER
        val hasNext: Boolean get() = nextValue != NULL_POINTER
        val isNull: Boolean
            get() = position == NULL_POINTER

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

    var cap: Int = 0
        get() {
            if (field == 0) {
                field = fileMapper.position(CAP).readInt()
            }
            return field
        }
        set(value) {
            field = value
            fileMapper.position(CAP).writeInt(value)

        }
    var indexArrayPosition: Long
        get() = fileMapper.position(INDEX_ARRAY_Position).readLong()
        set(value) {

            fileMapper.position(INDEX_ARRAY_Position).writeLong(value)
        }

    val indexArray: IndexArray
        get() = IndexArray(indexArrayPosition, cap)

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
        usageFileSize = DATA_AREA_START.toLong()
        //设置文件类型
        fileMapper.position(TYPE_MARK).writeLong(FileType.MutableMap)
        cap = initCap
        //分配索引数组
        val indexArraySize = (IndexPointer.POINTER_LENGTH * cap.toLong())
        val alloc = alloc(indexArraySize)
        indexArrayPosition = alloc
    }

    fun expansion(newCap: Int) {
        val oldPosition = indexArrayPosition
        val oldCap = cap
        cap = newCap
        val diff = (newCap - oldCap) * IndexPointer.POINTER_LENGTH.toLong()
        while (usageFileSize + diff > fileSize) {
            resizeFile()
        }

        val newPosition = alloc(diff)
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

        val newPointer = dataBlock.pointer
        //没有同hash的节点
        if (!indexPointerNode.hasNext) {
            indexPointerNode.next = newPointer
            newPointer.prev = indexPointerNode
        }
        //hash冲突了
        else {
            var lastPointer = indexPointerNode.next
            while (!lastPointer.isNull){

                val currentOldData = DataBlock(lastPointer)
                //同key 进行替换
                if (currentOldData.hashCode == dataBlock.hashCode && currentOldData.key == dataBlock.key) {
                    //替换前后节点的指针
                    currentOldData.delete = true
                    val oldPointer = currentOldData.pointer
                    val prev = oldPointer.prev
                    //修改前指针
                    prev.next = newPointer
                    newPointer.prev = prev

                    //修改后指针
                    if (oldPointer.hasNext) {
                        oldPointer.next.prev = newPointer
                        newPointer.next = oldPointer.next
                    }

                    return
                }
                lastPointer = lastPointer.next
            }
            //没有重复的节点
            lastPointer.next = newPointer
            newPointer.prev = lastPointer
        }
    }


    private fun hash(hashCode: Int, cap: Int): Int {
        return hashCode % cap
    }

    override var size: Int
        get() = fileMapper.position(SIZE).readLong().toInt()
        set(value) {
            fileMapper.position(SIZE).writeLong(value.toLong())
        }
    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() {
            val set = mutableSetOf<MutableMap.MutableEntry<K, V>>()
            forEachEntry {
                set.add(object : MutableMap.MutableEntry<K, V> {
                    override val key: K
                        get() = it.key
                    override val value: V
                        get() = it.value

                    override fun setValue(newValue: V): V {
                        return put(it.key, newValue)!!
                    }

                })
            }
            return set
        }
    override val keys: MutableSet<K>
        get() {
            val mutableListOf = mutableSetOf<K>()
            forEachEntry {
                mutableListOf.add(it.key)
            }
            return mutableListOf
        }
    override val values: MutableCollection<V>
        get() {
            val mutableListOf = mutableListOf<V>()
            forEachEntry {
                mutableListOf.add(it.value)
            }
            return mutableListOf
        }


    override fun containsKey(key: K): Boolean {
        return find(key) != null
    }


    /**
     * block：  返回false表示遍历终止
     */
    private fun forEachEntry(block: (DataBlock) -> Boolean) {
        for (hash in 0 until cap) {
            var pointer = indexArray[hash]
            while (pointer.hasNext) {
                pointer = pointer.next
                val dataBlock = DataBlock(pointer)
                if (!block(dataBlock)) {
                    return
                }
            }
        }
    }

    override fun containsValue(value: V): Boolean {
        var find = false
        forEachEntry {
            if (it.value == value) {
                find = true
                return@forEachEntry false
            }
            return@forEachEntry true
        }
        return find;
    }

    override fun get(key: K): V? {
        return find(key)?.value
    }

    private fun find(key: K): DataBlock? {
        val hashCode = key.hashCode()
        val hash = hash(hashCode, cap)
        var pointer = indexArray[hash]
        while (pointer.hasNext) {
            pointer = pointer.next
            val dataBlock = DataBlock(pointer)
            if (dataBlock.hashCode == hashCode && dataBlock.key == key) {
                return dataBlock;
            }
        }
        return null;
    }

    override fun isEmpty(): Boolean {
        return size == 0
    }

    override fun clear() {
        forEachEntry {
            it.delete = true
            return@forEachEntry true
        }
        for (i in 0 until size) {
            indexArray[i].nextValue = NULL_POINTER
        }


    }

    override fun put(key: K, value: V): V? {
        val dataBlock = DataBlock(key, value)
        insert(dataBlock, IndexArray(indexArrayPosition, cap))
        return value
    }

    override fun putAll(from: Map<out K, V>) {
        TODO("Not yet implemented")
    }

    override fun remove(key: K): V? {
        val targetDataBlock = find(key) ?: return null
        val pointer = targetDataBlock.pointer
        pointer.prev.next = pointer.next
        if (pointer.hasNext) {
            pointer.next.prev = pointer.prev
        }
        targetDataBlock.delete = true
        return targetDataBlock.value

    }


}