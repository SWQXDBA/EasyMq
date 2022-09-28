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
    initCap: Int = 5000,

    /**
     * value字节数量的冗余倍率。
     * 创建数据块时会为value分配额外的空间以便将来新的value进行替换
     * 如果冗余替换成功，则在插入新的键值对时不需要重新分配额外的空间，可以避免文件大小迅速膨胀
     * 0.5f意味着会为value分配1.5倍大小的空间
     *
     * 设置为负数则动态调整倍率
     */
    val valueRedundancyBytesRatio: Float = -1f,
    fileMapperType: FileMapperType = FileMapperType.MemoryMapMapper
) : PersistenceMap<K, V>, AbstractFilePersistenceCollection(filePath, autoForceMills, forcePerOption, initCap,fileMapperType) {


    companion object {
        //文件类型标记
        private const val TYPE_MARK = 0

        //记录索引数组的容量
        private const val CAP = 8


        //记录元素数量
        private const val SIZE = 12

        //记录索引数组的位置
        private const val INDEX_ARRAY_Position = 16

        //记录文件已使用的字节大小
        private const val USAGE = 24

        //保留位
        private const val RESERVED = 32

        //数据区起始位置
        private const val DATA_AREA_START = 48

        //扩容因子
        private const val expansionPercent = 0.75

        //遍历方式切换阈值
        private const val batchForeachMinSize = 1000

        private object DATA_AREA {
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

        private object INDEX_ARRAY {
            //delete标志位 扩容的时候旧的索引数组会被删除
            val DELETE_MARK = 0

            //数据大小
            val DATA_SIZE = 1

            //索引指针数组的起始位置
            val ARRAY_START = 9
        }

        private object IndexPointer {

            //所占大小为16字节
            val POINTER_LENGTH = LongBytesSize * 2

            //前指针 索引指针没有前驱节点 所以前指针必须为-1 代表寻找结束
            val PREV_POINTER = 0

            //后指针
            val NEXT_POINTER = 8
        }

        //代表空指针
        private val NULL_POINTER: Long = 0;
    }

    override fun computeIndexArraySize(cap: Int): Long {
        return (INDEX_ARRAY.ARRAY_START + cap * IndexPointer.POINTER_LENGTH).toLong()
    }

    inner class IndexArray(position: Long, cap: Int) :
        AbstractFilePersistenceCollection.IndexArray<DoublePointer>(position, cap) {

        val head: Header
            get() = Header(position)

        override operator fun get(index: Int): DoublePointer {
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

        fun canRead(): Boolean {
            return position < usageFileSize
        }

    }

    protected inner class DataBlock private constructor() {
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
        constructor(key:K ,valueBytes:ByteArray):this(){

            val keyBytes = keySerializer.toBytes(key)


            var size = keyBytes.size + valueBytes.size + DATA_AREA.META_LENGTH




            if(valueRedundancyBytesRatio>0){
                size += (valueBytes.size * valueRedundancyBytesRatio).toLong()
            }else{

                val oneMb = 1024*1024
                size += when {
                    valueBytes.size<64 -> {
                        /**
                         * 较小的value可能是基本类型，或者小的记录 一般修改时大小不会有明显变化
                         */
                        4
                    }
                    valueBytes.size<1024*1024*64 -> {
                        /**
                         * 中等大小的数据
                         */
                        (valueBytes.size*0.5f).toInt()
                    }
                    else -> {

                        /**
                         * 如果大于64mb 则认为可能存的是文本，音频等大文件 此类文件一般不会频繁修改 只分配少量冗余空间
                         */
                        1024*1024
                    }
                }
            }



            this.position = alloc(size)



            this.delete = false

            keySize = keyBytes.size
            valueSize = valueBytes.size

            dataSize = size

            hashCode = key.hashCode()

            fileMapper.position(keyStart).writeBytes(keyBytes)
            fileMapper.position(valueStart).writeBytes(valueBytes)


            //必要的代码 因为压缩操作后, 指针初始值不一定是0
            pointer.nextValue = NULL_POINTER
            pointer.prevValue = NULL_POINTER
        }
        /**
         * 创建一个不存在的节点，为其分配空间
         */
        constructor(key: K, value: V) : this(key,valueSerializer.toBytes(value))

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

        //可以给value用的字节数
        val valueFreeSize: Int
            get() = (dataSize - DATA_AREA.META_LENGTH - keySize).toInt()

        fun tryReplaceValue(newBytes: ByteArray): Boolean {
            val freeSize = valueFreeSize


            if (newBytes.size <= freeSize) {
                valueSize = newBytes.size
                fileMapper.position(valueStart).writeBytes(newBytes)
                return true
            }
            return false
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

    protected inner class PersistenceMutableEntry(val dataBlock: DataBlock) : MutableMap.MutableEntry<K, V> {
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

    override var usageFileSize: Long
        get() = fileMapper.position(USAGE).readLong()
        set(value) {
            fileMapper.position(USAGE).writeLong(value)
        }

    override var cap: Int = 0
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
    override var indexArrayPosition: Long
        get() = fileMapper.position(INDEX_ARRAY_Position).readLong()
        set(value) {
            fileMapper.position(INDEX_ARRAY_Position).writeLong(value)
        }

    override val indexArray: IndexArray
        get() = IndexArray(indexArrayPosition, cap)


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
        indexArray.head.delete = false
    }

    /**
     * 给hash array扩容
     */
    override fun expansion(newCap: Int) {


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
        newArray.head.delete = false


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

    /**
     * 把数据块插入索引数组中 索引数组可以是新的 也可以是旧的 这个方法可以在扩容时调用 也可以在插入新数据的时候调用
     * 现在只用作从旧的hash数组中插入到新的hash数组中 因为如果是新的元素 insertNew的效率更高
     */
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

    /**
     * 插入一个新的键值对 这个方法的意义在于如果新的value的字节数可以装在旧的数据块中，则允许value原地替换。
     */
    private fun insertNew(key: K, value: V, indexArray: IndexArray) {
        val hashIndex = hash(key.hashCode(), indexArray.cap)
        val indexPointerNode = indexArray[hashIndex]


        val dataBlock: DataBlock
        val newPointer: DoublePointer


        //没有同hash的节点
        if (!indexPointerNode.hasNext) {
            dataBlock = DataBlock(key, value)
            newPointer = dataBlock.pointer
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
                if (currentOldData.hashCode == key.hashCode() && currentOldData.key == key) {

                    val valueBytes = valueSerializer.toBytes(value)
                    //原地替换
                    if (!currentOldData.tryReplaceValue(valueBytes)) {
                        //原地替换失败 使用新节点替换
                        dataBlock = DataBlock(key, valueBytes)
                        newPointer = dataBlock.pointer

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
                    }
                    return
                }
            } while (lastPointer.hasNext)


            dataBlock = DataBlock(key, value)
            newPointer = dataBlock.pointer
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
            val set = object : MutableSet<MutableMap.MutableEntry<K, V>> {
                override fun add(element: MutableMap.MutableEntry<K, V>): Boolean {
                    return this@FilePersistenceMap.put(element.key, element.value) != null
                }

                override fun addAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var success = true
                    elements.forEach { success = success and add(it) }
                    return success
                }

                override fun clear() {
                    this@FilePersistenceMap.clear()
                }

                override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> {
                    return object : MutableIterator<MutableMap.MutableEntry<K, V>> {
                        var head = Header(DATA_AREA_START.toLong())
                        override fun hasNext(): Boolean {
                            while ((head.delete || head.isIndexArray()) && head.canRead()) {
                                head = head.next
                            }
                            return head.canRead()
                        }

                        override fun next(): MutableMap.MutableEntry<K, V> {
                            if (!hasNext()) {
                                throw IndexOutOfBoundsException("don't has next!")
                            }
                            val entry = PersistenceMutableEntry(DataBlock(head.position))
                            head = head.next
                            return entry
                        }

                        override fun remove() {
                            this@FilePersistenceMap.removeDataBlock(DataBlock(head.position))
                        }

                    }
                }

                override fun remove(element: MutableMap.MutableEntry<K, V>): Boolean {
                    return this@FilePersistenceMap.remove(element.key, element.value)
                }

                override fun removeAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var success = true
                    elements.forEach { success = success and this@FilePersistenceMap.remove(it.key, it.value) }
                    return success
                }

                override fun retainAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var modify = false
                    val tempSet = elements.map { Pair(it.key, it.value) }.toSet()
                    //寻找要删除的元素
                    val toRemove = mutableSetOf<K>()
                    this@FilePersistenceMap.forEachEntry {

                        if (!tempSet.contains(Pair(it.key, it.value))) {
                            modify = true
                            toRemove.add(it.key)
                        }
                        return@forEachEntry true
                    }
                    toRemove.forEach {

                        this@FilePersistenceMap.remove(it)
                    }

                    return modify
                }

                override val size: Int
                    get() = this@FilePersistenceMap.size

                override fun contains(element: MutableMap.MutableEntry<K, V>): Boolean {
                    return this@FilePersistenceMap.contains(element.key, element.value)
                }

                override fun containsAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                    var containsAll = true
                    elements.forEach { containsAll = containsAll and contains(it) }
                    return containsAll
                }

                override fun isEmpty(): Boolean {
                    return size == 0
                }

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

    fun contains(key: K, value: V): Boolean {
        val dataBlock = find(key)
        return dataBlock != null && dataBlock.value == value
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

        while (head.position < usageFileSize) {

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
                    indexArrayPosition -= space
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
//        fileMapper.position(usageFileSize - space).writeBytes(ByteArray(space.toInt()))
        usageFileSize -= space
        return space
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Map<*, *> || size != other.size) {
            return false
        }

        return allSuccess(entries) {

            other[it.key] == it.value

        } && allSuccess(other.entries) {

            this[it.key] == it.value
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

        for (i in 0 until cap) {
            indexArray[i].nextValue = NULL_POINTER
        }

        size = 0
    }


    override fun put(key: K, value: V): V? {


        if (size >= cap * expansionPercent) {
            expansion(cap * 2)
        }



        insertNew(key, value, indexArray)



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

    private fun removeDataBlock(targetDataBlock: DataBlock): V? {
        size--
        val pointer = targetDataBlock.pointer
        pointer.prev.next = pointer.next
        if (pointer.hasNext) {
            pointer.next.prev = pointer.prev
        }
        targetDataBlock.delete = true
        return targetDataBlock.value
    }

    fun remove(key: K, value: V): Boolean {

        val targetDataBlock = find(key) ?: return false
        if (targetDataBlock.value != value) {
            return false
        }
        return removeDataBlock(targetDataBlock) != null

    }

    fun retainAll(pairs: Collection<Pair<K, V>>): Boolean {
        val set = pairs.toSet()
        var modify = false
        val keysToRemove = mutableSetOf<Pair<K, V>>()

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
        var c = 0
        forEachEntry {
            c++
            stringBuilder.append("{${it.key} : ${it.value} =>${it.position}} ")
            true
        }
//        forEach { t, u ->
//            stringBuilder.append("{$t : $u} ")
//        }
        stringBuilder.append("=>>$c }")
        return stringBuilder.toString()
    }

}