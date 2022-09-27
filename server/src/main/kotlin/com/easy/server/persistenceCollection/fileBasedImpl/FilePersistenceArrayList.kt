package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.*
import java.awt.Point
import kotlin.properties.Delegates

class FilePersistenceArrayList<E>(
    filePath: String,
    val serializer: Serializer<E>,
    autoForceMills: Long = 10,
    forcePerOption: Boolean = false,
    initCap: Int = 16
) : PersistenceList<E>, AbstractFilePersistenceCollection(filePath, autoForceMills, forcePerOption, initCap) {
    override fun equals(other: Any?): Boolean {
        if(other !is  List<*>||size!=other.size){
            return false
        }
        for(i in 0 until size){
            if(get(i)!=other[i]){
                return false
            }
        }
        return true
    }

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

        //遍历方式切换阈值
        const val batchForeachMinSize = 100

        object DATA_AREA {
            //除了数据本身外其他信息所占空间
            const val META_LENGTH: Long = 25

            //delete标志位 1代表删除
            const val DELETE_MARK = 0

            //数据大小
            const val DATA_SIZE = 1


            //指向索引数组的指针
            const val INDEX_POINTER = 9

            const val HASH_CODE = 17

            //值大小
            const val VALUE_SIZE = 21

            const val VALUE_START = 25
        }

        object INDEX_ARRAY {
            //delete标志位 扩容的时候旧的索引数组会被删除
            val DELETE_MARK = 0

            //数据大小
            val DATA_SIZE = 1

            //索引指针数组的起始位置
            val ARRAY_START = 9

            val POINTER_LENGTH = LongBytesSize
        }

        //代表空指针
        val NULL_POINTER: Long = 0;
    }

    init {

        val type = fileMapper.position(TYPE_MARK).readLong()
        if (type == FileType.NEW_FILE) {
            init()
        } else if (type != FileType.ArrayList) {
            throw Exception("文件类型不匹配！需要 ${FileType.MutableMap} 但是检测为$type")
        }
    }

    fun init() {
        usageFileSize = DATA_AREA_START.toLong()
        //设置文件类型
        fileMapper.position(TYPE_MARK).writeLong(FileType.ArrayList)
        cap = initCap


        //分配索引数组
        val indexArraySize = computeIndexArraySize(initCap)
        val alloc = alloc(indexArraySize)
        indexArrayPosition = alloc
        indexArray.head.dataSize = indexArraySize
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

    override fun computeIndexArraySize(cap: Int): Long {
        return INDEX_ARRAY.ARRAY_START + cap * INDEX_ARRAY.POINTER_LENGTH.toLong()
    }

    override var size: Int
        get() = fileMapper.position(SIZE).readInt()
        set(value) {
            fileMapper.position(SIZE).writeInt(value)
        }

    //代表索引数组的一个节点
    inner class Pointer(val position: Long, val index: Int) {

        fun isNull(): Boolean {
            return value == NULL_POINTER
        }

        var value: Long
            get() = fileMapper.position(position).readLong()
            set(value) {
                fileMapper.position(position).writeLong(value)
            }

        //数组移除元素
        fun remove() {
            val block = getBlock()
            block.delete = true
            this.value = NULL_POINTER
            val start = position + INDEX_ARRAY.POINTER_LENGTH

            //维护数据指向数组的指针
            for (i in index + 1 until size) {
                indexArray[i].getBlock().indexPointer -= INDEX_ARRAY.POINTER_LENGTH
            }
            fileMapper.moveBytes(
                start,
                (indexArrayPosition + INDEX_ARRAY.ARRAY_START + size * INDEX_ARRAY.POINTER_LENGTH - start).toInt(),
                -INDEX_ARRAY.POINTER_LENGTH.toLong()
            )
            size--

        }

        fun replace(dataBlock: DataBlock) {
            val block = getBlock()
            block.delete = true
            this.value = dataBlock.position
            getBlock().indexPointer = position
        }

        fun getBlock(): DataBlock {
            return DataBlock(value)
        }
    }

    inner class IndexArray(position: Long, cap: Int) :
        AbstractFilePersistenceCollection.IndexArray<Pointer>(position, cap) {
        val head: BlockHeader
            get() = BlockHeader(position)

        override fun get(index: Int): Pointer {

            return Pointer(position + INDEX_ARRAY.ARRAY_START + INDEX_ARRAY.POINTER_LENGTH * index, index)
        }

        override fun toString(): String {

            val stringBuilder = StringBuilder()
            stringBuilder.append("[")
            for (i in 0 until size) {
                stringBuilder.append("${position + i * INDEX_ARRAY.POINTER_LENGTH}: ${this[i].value} ")
            }
            stringBuilder.append("]")
            return stringBuilder.toString()
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

        val head: BlockHeader
            get() = BlockHeader(position)

        /**
         * 创建一个不存在的节点，为其分配空间
         */
        constructor(element: E) : this() {


            val elementBytes = serializer.toBytes(element)

            val size = elementBytes.size + DATA_AREA.META_LENGTH

            this.position = alloc(size)


            this.delete = false


            valueSize = elementBytes.size

            dataSize = size

            hashCode = element.hashCode()


            fileMapper.position(valueStart).writeBytes(elementBytes)


        }


        var indexPointer: Long
            get() = fileMapper.position(position + DATA_AREA.INDEX_POINTER).readLong()
            set(value) {
                fileMapper.position(position + DATA_AREA.INDEX_POINTER).writeLong(value)
            }

        var delete: Boolean
            get() = fileMapper.position(position + DATA_AREA.DELETE_MARK).readByte() != 0.toByte()
            set(value) {
                val v: Byte = if (value) 1 else 0
                fileMapper.position(position + DATA_AREA.DELETE_MARK).writeByte(v)
            }


        var dataSize: Long
            get() = fileMapper.position(position + DATA_AREA.DATA_SIZE).readLong()
            private set(value) {

                fileMapper.position(position + DATA_AREA.DATA_SIZE).writeLong(value)

            }


        var valueSize: Int
            get() = fileMapper.position(position + DATA_AREA.VALUE_SIZE).readInt()
            private set(value) {
                fileMapper.position(position + DATA_AREA.VALUE_SIZE).writeInt(value)
            }
        var hashCode: Int
            get() = fileMapper.position(position + DATA_AREA.HASH_CODE).readInt()
            private set(value) {
                fileMapper.position(position + DATA_AREA.HASH_CODE).writeInt(value)
            }

        private val valueStart: Long
            get() = position + DATA_AREA.VALUE_START.toLong()

        val value: E
            get() {
                return serializer.fromBytes(fileMapper.byteArrayAt(valueStart, valueSize))
            }
    }

    inner class BlockHeader(val position: Long) {

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

        val next: BlockHeader
            get() = BlockHeader(position + dataSize)

        fun canRead(): Boolean {
            return position < usageFileSize
        }

    }

    /**
     * 压缩文件
     * return 压缩了多少bytes
     */
    fun compress(): Long {

        var space = 0L
        var head = BlockHeader(DATA_AREA_START.toLong())

        while (head.position < usageFileSize) {


            val next = head.next


            if (head.delete) {
                space += head.dataSize
            } else {
                //如果这个数据块是索引数组
                if (head.isIndexArray()) {

                    //修改数据指向索引的指针
                    for (i in 0 until size) {
                        val pointer = indexArray[i]
                        pointer.getBlock().indexPointer -= space
                    }
                    indexArrayPosition -= space
                } else {
                    //是普通数据块 修改索引指向它的指针

                    val pointer = DataBlock(head.position).indexPointer

                    fileMapper.position(pointer).writeLong(head.position - space)
                }

                fileMapper.moveBytes(head.position, head.dataSize.toInt(), -space)
            }
            head = next
        }
        usageFileSize -= space
        return space
    }

    /**
     * 分配新的空间 然后复制数组即可
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
        oldArray.head.delete = true


        fileMapper.moveBytes(
            oldPosition + INDEX_ARRAY.ARRAY_START, oldCap * INDEX_ARRAY.POINTER_LENGTH,
            newPosition - oldPosition
        )
        //维护数据指向索引的指针
        for (i in 0 until size) {
            indexArray[i].getBlock().indexPointer += (newPosition - oldPosition)
        }
    }


    override fun contains(element: E): Boolean {
        search(element) ?: return false
        return true
    }

    override fun containsAll(elements: Collection<E>): Boolean {
        return allSuccess(elements) {
            contains(it)
        }
    }

    override fun get(index: Int): E {
        checkIndex(index)

        return indexArray[index].getBlock().value
    }

    override fun indexOf(element: E): Int {
        val pointer = search(element) ?: return -1
        return pointer.index
    }

    override fun isEmpty(): Boolean {
        return size == 0
    }

    override fun iterator(): MutableIterator<E> {
        return listIterator()
    }

    override fun lastIndexOf(element: E): Int {
        val pointer = search(element, size - 1 downTo 0) ?: return -1
        return pointer.index
    }

    override fun add(element: E): Boolean {
        add(size, element)
        return true
    }

    override fun add(index: Int, element: E) {

        if (index < 0 || index > size) {
            throw java.lang.IndexOutOfBoundsException("error index  $index")
        }

        if (size == cap) {
            expansion(cap * 2)
        }
        val dataBlock = DataBlock(element)

        //维护数据指向索引的指针
        for (i in index + 1 until size) {
            indexArray[i].getBlock().indexPointer += INDEX_ARRAY.POINTER_LENGTH
        }

        fileMapper.moveBytes(
            indexArray[index].position,
            (size - index) * INDEX_ARRAY.POINTER_LENGTH,
            INDEX_ARRAY.POINTER_LENGTH.toLong()
        )

        indexArray[index].value = dataBlock.position
        dataBlock.indexPointer = indexArray[index].position


        size++
    }


    override fun addAll(index: Int, elements: Collection<E>): Boolean {
        elements.forEach {
            add(index, it)
        }
        return true
    }

    override fun addAll(elements: Collection<E>): Boolean {
        elements.forEach {
            add(size - 1, it)
        }
        return true
    }

    override fun clear() {
        while (!isEmpty()) {
            indexArray[size-1].remove()
        }

    }

    override fun listIterator(): MutableListIterator<E> {
        return listIterator(0)
    }

    override fun listIterator(index: Int): MutableListIterator<E> {
        return object : MutableListIterator<E> {
            var current = index

            override fun hasPrevious(): Boolean {
                return current > 0
            }

            override fun nextIndex(): Int {
                return current + 1
            }

            override fun previous(): E {
                current--
                return get(current)
            }

            override fun previousIndex(): Int {
                return current - 1
            }

            override fun add(element: E) {
                add(current, element)
            }

            override fun hasNext(): Boolean {
                return current < size
            }

            override fun next(): E {
                val currentElement = get(current)
                current++
                return currentElement
            }

            override fun remove() {
                removeAt(current)
            }

            override fun set(element: E) {
                set(current, element)
            }

        }
    }

    private fun search(element: E, progression: IntProgression = 0 until size): Pointer? {
        for (i in progression) {
            val pointer = indexArray[i]
            if (!pointer.isNull()) {
                val block = pointer.getBlock()
                if (block.hashCode == element.hashCode() && block.value == element) {
                    return pointer
                }
            }
        }
        return null
    }

    override fun remove(element: E): Boolean {
        val pointer = search(element) ?: return false
        pointer.remove()

        return true
    }

    override fun removeAll(elements: Collection<E>): Boolean {
        return allSuccess(elements) {
            remove(it)
        }
    }

    override fun removeAt(index: Int): E {
        checkIndex(index)
        val pointer = indexArray[index]
        val value = pointer.getBlock().value
        pointer.remove()
        return value
    }

    override fun retainAll(elements: Collection<E>): Boolean {
        return retainAll(elements, iterator())
    }

    private fun checkIndex(index: Int) {
        if (index < 0 || index >= size) {
            throw java.lang.IndexOutOfBoundsException("error index  $index")
        }
    }

    override fun set(index: Int, element: E): E {
        checkIndex(index)
        val pointer = indexArray[index]
        val dataBlock = DataBlock(element)
        pointer.replace(dataBlock)
        return element
    }

    /**
     * 此方法不返回一个view 而是返回一个新的集合
     */
    override fun subList(fromIndex: Int, toIndex: Int): MutableList<E> {
        checkIndex(fromIndex)
        checkIndex(toIndex - 1)
        val mutableList = mutableListOf<E>()
        for (i in fromIndex until toIndex) {
            mutableList.add(indexArray[i].getBlock().value)
        }
        return mutableList
    }

    override fun toString(): String {
        return iterToString(iterator())
    }


}