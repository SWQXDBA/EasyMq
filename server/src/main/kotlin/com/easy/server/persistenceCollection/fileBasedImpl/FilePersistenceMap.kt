package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.*
import kotlin.math.abs
import kotlin.properties.Delegates

open class FilePersistenceMap<K, V>(
    filePath: String,
    val keySerializer: Serializer<K>,
    val valueSerializer: Serializer<V>,
    autoForceMills: Long = 10,
    forcePerOption: Boolean = false,
    val initCap: Int = 16
) : PersistenceMap<K, V>, FilePersistenceCollection(filePath, autoForceMills, forcePerOption) {


    companion object {
        //文件类型标记
        const val TYPE_MARK = 0

        //记录索引数组的容量
        const val CAP = 8


        //记录元素数量
        const val SIZE = 12

        //记录索引数组的位置
        const val INDEX_ARRAY_Position = 16

        //记录文件已使用的字节大小
        const val USAGE = 24

        //保留位
        const val RESERVED = 32

        //数据区起始位置
        const val DATA_AREA_START = 48

        //扩容因子
        const val expansionPercent = 0.75

        //遍历方式切换阈值
        const val batchForeachMinSize = 100

        object DATA_AREA {
            /**HEADER**/
            //除了数据本身外其他信息所占空间
            const val META_LENGTH: Long = 37

            //delete标志位 1代表删除
            const val DELETE_MARK = 0

            //数据大小
            const val DATA_SIZE = 1

            /**HEADER END**/
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

    fun computeIndexArraySize(cap: Int): Long {
        return (INDEX_ARRAY.ARRAY_START + cap * IndexPointer.POINTER_LENGTH).toLong()
    }

    inner class IndexArray(val position: Long, val cap: Int) {

        val head: Header
            get() = Header(position)


        operator fun get(index: Int): DoublePointer {
            return DoublePointer(position + INDEX_ARRAY.ARRAY_START + index * IndexPointer.POINTER_LENGTH)
        }

        override fun toString(): String {

            val stringBuilder = StringBuilder()



            for (i in 0 until cap) {
                stringBuilder.append("$i: {${this[i].prevValue}<->${this[i].nextValue}} ")
            }

            return stringBuilder.toString()
        }

    }


    /**
     * 表示一个DataBlock 或者IndexArray的开头部分
     */
    inner class Header(val position: Long) {

        var delete: Boolean
            get() = fileMapper.position(position + DATA_AREA.DELETE_MARK).readByte() != 0.toByte()
            set(value) {
                val v: Byte = if (value) 1 else 0
                fileMapper.position(position + DATA_AREA.DELETE_MARK).writeByte(v)
            }

        var dataSize: Long
            get() {

                return fileMapper.position(position + DATA_AREA.DATA_SIZE).readLong()
            }
            set(value) {
                fileMapper.position(position + DATA_AREA.DATA_SIZE).writeLong(value)
            }

        fun isIndexArray(): Boolean {
            return position == indexArrayPosition
        }

        val next: Header
            get() = Header(position + dataSize)
        fun canRead():Boolean{
            return position <usageFileSize
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

            val size = keyBytes.size + valueBytes.size + DATA_AREA.META_LENGTH

            this.position = alloc(size)


            this.delete = false

            keySize = keyBytes.size
            valueSize = valueBytes.size

            dataSize = size

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

        val pointer: DoublePointer get() = DoublePointer(position + DATA_AREA.POINTER)

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
            get() = keySerializer.fromBytes(fileMapper.byteArrayAt(keyStart, keySize))!!

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

    inner class PersistenceMutableEntry(val dataBlock: DataBlock):MutableMap.MutableEntry<K, V>{
        override val key: K
            get() = dataBlock.key
        override val value: V
            get() = dataBlock.value

        override fun setValue(newValue: V): V {
            return this@FilePersistenceMap.put(dataBlock.key, newValue)!!
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
        while (start + size >= fileSize) {

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
        val indexArraySize = computeIndexArraySize(initCap)
        val alloc = alloc(indexArraySize)
        indexArrayPosition = alloc
        indexArray.head.dataSize = indexArraySize
    }

    /**
     * 给hash array扩容
     */
    fun expansion(newCap: Int) {

        val oldPosition = indexArrayPosition
        val oldCap = cap
        cap = newCap
        val diff = computeIndexArraySize(newCap)
        while (usageFileSize + diff > fileSize) {
            resizeFile()
        }

        val newPosition = alloc(diff)
        indexArrayPosition = newPosition

        val newArray = IndexArray(newPosition, newCap)
        newArray.head.dataSize = diff

        val oldArray = IndexArray(oldPosition, oldCap)
        for (i in 0 until oldCap) {
            var next = oldArray[i].next
            while (!next.isNull) {
                val current = next

                next = current.next
                current.nextValue = NULL_POINTER
                current.prevValue = NULL_POINTER
                size--
                insert(DataBlock(current), newArray)
            }
        }
        oldArray.head.delete = true


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
            var lastPointer = indexPointerNode
            do {
                lastPointer = lastPointer.next
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
            } while (lastPointer.hasNext)
            //没有重复的节点
            lastPointer.next = newPointer
            newPointer.prev = lastPointer

        }
        size++
    }


    private fun hash(hashCode: Int, cap: Int): Int {
        return abs(hashCode) % cap
    }

    override var size: Int
        get() = fileMapper.position(SIZE).readInt()
        set(value) {
            fileMapper.position(SIZE).writeInt(value)
        }
    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() {



            val set =object :MutableSet<MutableMap.MutableEntry<K, V>>{
                override fun add(element: MutableMap.MutableEntry<K, V>): Boolean {
                   return this@FilePersistenceMap.put(element.key,element.value)!=null
                }

                override fun addAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var success = true
                    elements.forEach { success = success and  add(it)}
                    return success
                }

                override fun clear() {
                   this@FilePersistenceMap.clear()
                }

                override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> {
                    return object : MutableIterator<MutableMap.MutableEntry<K, V>>{
                        var head = Header(DATA_AREA_START.toLong())
                        override fun hasNext(): Boolean {
                            while(head.delete||head.isIndexArray()){
                                head = head.next
                            }
                            return head.canRead()
                        }
                        override fun next(): MutableMap.MutableEntry<K, V> {
                            if(!hasNext()){
                                throw IndexOutOfBoundsException("don't has next!")
                            }
                            val entry = PersistenceMutableEntry(DataBlock(head.position))
                            head = head.next
                            return  entry
                        }

                        override fun remove() {
                            this@FilePersistenceMap.removeDataBlock(DataBlock(head.position))
                        }

                    }
                }

                override fun remove(element: MutableMap.MutableEntry<K, V>): Boolean {
                    return this@FilePersistenceMap.remove(element.key,element.value)
                }

                override fun removeAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var success = true
                    elements.forEach { success = success and  this@FilePersistenceMap.remove(it.key,it.value)}
                    return success
                }

                override fun retainAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var modify = false
                    val tempSet = elements.map { Pair(it.key, it.value) }.toSet()
                    //寻找要删除的元素
                    val toRemove = mutableSetOf<K>()
                    this@FilePersistenceMap.forEachEntry {

                        if(!tempSet.contains( Pair(it.key, it.value))){
                                modify = true
                            toRemove.add(it.key)
                        }
                            return@forEachEntry true
                    }
                    toRemove.forEach {

                        this@FilePersistenceMap.remove(it) }

                    return modify
                }

                override val size: Int
                    get() = this@FilePersistenceMap.size

                override fun contains(element: MutableMap.MutableEntry<K, V>): Boolean {
                    return this@FilePersistenceMap.contains(element.key,element.value)
                }

                override fun containsAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var containsAll = true
                    elements.forEach {containsAll = containsAll and contains(it) }
                    return containsAll
                }

                override fun isEmpty(): Boolean {
                 return  size==0
                }

            }
            forEachEntry {
                set.add(PersistenceMutableEntry(it))
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
                val value = it.value
                if (value != null) {
                    mutableListOf.add(value)
                }
                return@forEachEntry false
            }
            return mutableListOf
        }


    override fun containsKey(key: K): Boolean {
        return find(key) != null
    }
     fun contains(key: K,value: V): Boolean {
         val dataBlock = find(key)
         return dataBlock != null&&dataBlock.value==value
    }

    /**
     * block：  返回false表示遍历终止
     */
    private fun forEachEntry(block: (DataBlock) -> Boolean) {
        if (size > batchForeachMinSize) {
            batchForEachEntry(block)
            return
        }

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

    /**
     * 顺序遍历方法 而不是根据索引找，因为顺序访问磁盘速度更快，所以当元素较多时切换到此方法快速遍历
     * block：  返回false表示遍历终止
     */
    private fun batchForEachEntry(block: (DataBlock) -> Boolean) {
        var head = Header(DATA_AREA_START.toLong())
        while (head.dataSize != 0L) {

            if (head.delete || head.isIndexArray()) {
                head = head.next
                continue
            } else {
                if (!block(DataBlock(head.position))) {
                    return
                }
            }
            head = head.next
        }
    }

    /**
     * 压缩文件
     * return 压缩了多少bytes
     */
    fun compress(): Long {

        var space = 0L
        var head = Header(DATA_AREA_START.toLong())

        while (head.position <usageFileSize) {
            val next = head.next
            if (head.delete) {
                space += head.dataSize


            } else {
                //如果这个数据块是索引数组
                if (head.isIndexArray()) {

                    for (i in 0 until cap) {
                        val pointer = indexArray[i]
                        if (pointer.hasNext) {
                            pointer.next.prevValue = pointer.position - space
                        }
                    }
                    indexArrayPosition -=  space
                } else {
                    //是普通数据块 则修改前后指针
                    val pointer = DataBlock(head.position).pointer
                    pointer.prev.nextValue = pointer.position - space
                    if (pointer.hasNext) {
                        pointer.next.prevValue = pointer.position - space
                    }
                }

                fileMapper.moveBytes(head.position, head.dataSize.toInt(), -space)
            }
            head = next
        }
        usageFileSize -= space
        return space
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
        for (i in 0 until cap) {
            indexArray[i].nextValue = NULL_POINTER
        }
        size = 0
    }


    override fun put(key: K, value: V): V? {


        if (size >= cap * expansionPercent) {
            expansion(cap * 2)
        }

        val dataBlock = DataBlock(key, value)


        insert(dataBlock, IndexArray(indexArrayPosition, cap))


        return value
    }

    override fun putAll(from: Map<out K, V>) {
        from.forEach { k, v ->
            put(k, v)
        }


    }

    override fun remove(key: K): V? {
        val targetDataBlock = find(key) ?: return null
        return removeDataBlock(targetDataBlock)
    }
    private fun removeDataBlock(targetDataBlock:DataBlock): V?{
        size--
        val pointer = targetDataBlock.pointer
        pointer.prev.next = pointer.next
        if (pointer.hasNext) {
            pointer.next.prev = pointer.prev
        }
        targetDataBlock.delete = true
        return targetDataBlock.value
    }
    fun remove(key: K,value:V) :Boolean{

        val targetDataBlock = find(key) ?: return false
        if(targetDataBlock.value!=value){
            return false
        }
        return removeDataBlock(targetDataBlock)!=null

    }
    fun retainAll(pairs:Collection<Pair<K,V>>):Boolean{
        val set = pairs.toSet()
        var modify = false
        val keysToRemove = mutableSetOf<Pair<K,V>>()

        forEachEntry {
            val pair = Pair(it.key, it.value)
            if (!set.contains(pair)) {
                modify = true
                keysToRemove.add(pair)
            }
            return@forEachEntry true
        }
        for (pair in keysToRemove) {
            remove(pair.first)
        }

        return modify
    }
    override fun toString(): String {
        val stringBuilder = StringBuilder()
        stringBuilder.append("{")
        forEach { t, u ->
            stringBuilder.append("{$t : $u}")
        }
        stringBuilder.append("}")
        return stringBuilder.toString()
    }

}