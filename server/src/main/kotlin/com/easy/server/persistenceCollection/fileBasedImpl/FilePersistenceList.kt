package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.PersistenceList
import com.easy.server.persistenceCollection.Serializer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.collections.ArrayList
import kotlin.io.path.exists

/**
 * offsetIndex: 偏移索引，表示数据的数据区偏移量
 * index: 表示第几个元素
 */
class FilePersistenceList<E>(
    val filePath: String,
    val serializer: Serializer<E>
) : PersistenceList<E> {
    //数据块原信息
    data class BlockMetaData(
        val delete: Boolean,
        val dataHashCode: Int,
        val dataSize: Int,
        /**
         * 数据在文件中的偏移量
         */
        val dataFilePosition: Int
    )


    var fileMapper: MappedByteBuffer

    /**
     * 表示4个字节
     */
    val intByteSize = 4

    /***
     * 文件的大小 在内存中进行维护 重新加载时会读取
     */
    //1mb初始大小
    var fileSize = 1024 * 1024L

    override var size: Int
        get() = fileMapper.position(SIZE_POSITION).asIntBuffer().get()
        set(value) {

            fileMapper.position(SIZE_POSITION).asIntBuffer().put(value)
        }


     var indexCap: Int
        get() = fileMapper.position(INDEX_CAP_POSITION).asIntBuffer().get()
       private set(value) {
            fileMapper.position(INDEX_CAP_POSITION).asIntBuffer().put(value)
        }

    /**
     * 数据区起始位置在文件中的偏移量
     */
    val dataAreaPosition: Int
        get() = 4 * (indexCap + 2)

    /**
     * 已用文件大小
     */
     val usageFileSize: Int
        get() {
            if (isEmpty()) {
                return dataAreaPosition - 1
            }
            //找到
            val lastData = getOffsetIndexByIndex(size - 1)
            val (_, _, dataSize, dataFilePosition) = getBlockMetaByOffsetIndex(lastData)



            return dataFilePosition + dataSize
        }

    /**
     * 空闲文件大小
     */
    private val freeFileSize: Int
        get() = (fileSize - usageFileSize).toInt()

    /**
     * 数据区所占字节数
     * 如果数据区没有数据 则dataAreaPosition实际上会等于usageFileSize+1
     */
    private val dataAreaSize: Int
        get() {
            if (isEmpty()) {
                return 0
            }
            return usageFileSize - dataAreaPosition
        }

    companion object {
        //数据个数地址
        val SIZE_POSITION = 4

        //索引容量地址
        val INDEX_CAP_POSITION = 0

        //索引起始偏移量
        val OFFSET_INDEX_POSITION = 8

        //删除标志位在数据块中的偏移量
        val DELETE_OFFSET = 0

        //hashcode在数据块中的偏移量
        val DATA_HASHCODE_OFFSET = 1

        //数据大小在数据块中的偏移量
        val DATA_SIZE_OFFSET = 5

        //数据本体数据块中的偏移量
        val DATA_OFFSET = 9

        //数据块元数据所占大小
        val DATA_BLOCK_META_LENGTH = 9
    }


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
     * 根据偏移索引给出数据块在文件中的偏移量
     */
    private fun getDataBlockFilePositionByOffsetIndex(offsetIndex: Int): Int {
        return offsetIndex + dataAreaPosition
    }

    /**
     * 根据索引给出数据块在文件中的偏移量
     * index: 第几个元素
     */
    private fun getDataBlockFilePositionByIndex(index: Int): Int {
        return getOffsetIndexByIndex(index) + dataAreaPosition
    }

    /**
     * 根据索引(数据区偏移量)得到数据元信息
     */
    private fun getBlockMetaByOffsetIndex(offsetIndex: Int): BlockMetaData {

        //数据块在整个文件中的偏移量
        val dataBlockFilePosition = offsetIndex + dataAreaPosition

        val delete = fileMapper.position(dataBlockFilePosition + DELETE_OFFSET).get() == 1.toByte()
        val dataHashcode =
            fileMapper.position(dataBlockFilePosition + DATA_HASHCODE_OFFSET).asIntBuffer().get()



        val dataSize = fileMapper.position(dataBlockFilePosition + DATA_SIZE_OFFSET).asIntBuffer().get()


        return BlockMetaData(delete, dataHashcode, dataSize, dataBlockFilePosition + DATA_OFFSET)
    }

    /**
     * 根据索引得到数据元信息
     */
    private fun getBlockMetaByIndex(index: Int): BlockMetaData {
        return getBlockMetaByOffsetIndex(getOffsetIndexByIndex(index))
    }

    /**
     * 扩容文件为1.5倍
     */
    private fun resizeFile() {

        fileSize = (fileSize * 1.5).toLong()


        fileMapper = FileChannel.open(
            Path.of(filePath),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        ).map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
    }

    /**
     * 扩容偏移索引容量为2倍 需要后移数据区
     */
    private fun resizeIndexCap() {


        val oldCap = indexCap
        val newCap = if(oldCap==0) 2 else {oldCap * 2}
        val moveBytes = (newCap - oldCap) * 4
        move(dataAreaPosition, dataAreaSize, moveBytes)
        indexCap = newCap
    }

    /**
     * 把从 position开始length个字节 移动step个字节
     * position:起始位置 step: 移动多少个字节 正数代表后移 负数代表前移
     */
    private fun move(position: Int, length: Int, step: Int) {

        if (length == 0 || step == 0) {
            return
        }
        val temp = ByteArray(length)

        fileMapper.position(position).get(temp)
        fileMapper.position(position + step).put(temp)
    }

    /**
     * 获取尾部新元素的偏移量 用于插入元素
     */
    private fun getLastOffsetIndex(): Int {
        /**
         * 此时 没有元素了 则认为所有取数据都被废弃 从0开始使用
         */
        if (size == 0) {
            return 0
        }
        return dataAreaSize
    }

    /**
     * 根据index找到偏移索引,得出数据区偏移量
     * index: 第几个元素
     */
    private fun getOffsetIndexByIndex(index: Int): Int {

        return fileMapper.position(OFFSET_INDEX_POSITION + index * intByteSize).asIntBuffer().get()
    }

    /**
     * 根据index设置偏移索引
     * index: 第几个元素
     */
    private fun setOffsetIndexByIndex(index: Int, offsetIndex: Int) {
        fileMapper.position(OFFSET_INDEX_POSITION + index * intByteSize).asIntBuffer().put(offsetIndex)

  }

    /**
     * 根据index找到偏移索引 返回偏移索引在文件中的偏移量
     * index: 第几个元素
     */
    private fun getOffsetIndexPositionByIndex(index: Int): Int {

        return OFFSET_INDEX_POSITION + index * intByteSize
    }

    private fun getByteArray(position: Int, length: Int): ByteArray {

        val arr = ByteArray(length)
        fileMapper.position(position)
        fileMapper.get(arr)
        return arr
    }

    override fun contains(element: E): Boolean {

        return indexOf(element) != -1

    }

    override fun containsAll(elements: Collection<E>): Boolean {
        var match = true
        for (element in elements) {
            match = match and contains(element)
        }
        return match
    }

    override fun get(index: Int): E {

        val meta = getBlockMetaByIndex(index)

        val byteArray = getByteArray(meta.dataFilePosition, meta.dataSize)

        return serializer.fromBytes(byteArray)
    }

    private fun searchIndex(element: E, range: IntProgression): Int {
        for (i in range) {
            //根据索引 找到数据块的数据区偏移量
            val offset = getOffsetIndexByIndex(i)
            //读前9个byte 获取信息
            val meta = getBlockMetaByOffsetIndex(offset)
            //比较hashcode
            if (element.hashCode() == meta.dataHashCode) {

                val bytes = serializer.toBytes(element)
                //比较字节数
                if (bytes.size == meta.dataSize) {
                    val fromBytes = serializer.fromBytes(getByteArray(meta.dataFilePosition, meta.dataSize))
                    if (element == fromBytes) {
                        return i
                    }
                }
            }
        }
        return -1
    }

    override fun indexOf(element: E): Int {
        return searchIndex(element, 0 until size)
    }

    override fun isEmpty(): Boolean {
        return size == 0
    }


    override fun add(element: E): Boolean {
        add(size, element)
        return true
    }

    override fun add(index: Int, element: E) {
        //给索引空间扩容
        if (indexCap == size) {
            while (freeFileSize <= indexCap * intByteSize) {
                resizeFile()
            }
            resizeIndexCap()

        }


        val elementBytes = serializer.toBytes(element)
        //数据块大小
        val dataBlockSize = elementBytes.size + DATA_BLOCK_META_LENGTH
        //如果装不下新数据，对整个文件进行扩容
        while (dataBlockSize >= freeFileSize) {
            resizeFile()
        }

        //获取偏移索引
        val elementOffsetIndex: Int
        elementOffsetIndex = if (index == size) {
            getLastOffsetIndex()
        } else {
            //往中间插入的新元素的偏移量相当于上一个在这里的元素的偏移量
            getOffsetIndexByIndex(index)
        }

        /**
         * 需要先插入数据 再修改索引 否则读取数据区范围时会出错 因为获取usageSize需要找到最后一个元素
         */

        /**开始插入数据*/
        if (index != size) {
            //数据块的文件偏移量=数据区的文件偏移量+数据块在数据区的偏移量
            val start = dataAreaPosition + elementOffsetIndex
            //要移动的数据区长度=数据区大小-数据块起始位置在数据区的偏移量
            val length = dataAreaSize - elementOffsetIndex
            //此时 elementOffsetIndex上放的还是原先的数据 现在要集体后移
            move(
                start,
                length
                //移动量=新数据块的大小
                , dataBlockSize
            )
        }

        //插入元数据
        val dataBlockFilePosition = getDataBlockFilePositionByOffsetIndex(elementOffsetIndex)
        fileMapper.position(dataBlockFilePosition).put(0)
        fileMapper.position(dataBlockFilePosition + DATA_HASHCODE_OFFSET).asIntBuffer().put(element.hashCode())
        fileMapper.position(dataBlockFilePosition + DATA_SIZE_OFFSET).asIntBuffer().put(elementBytes.size)
        //插入数据
        fileMapper.position(dataBlockFilePosition + DATA_OFFSET).put(elementBytes)

        /**开始插入偏移索引*/
        //修改后面元素的偏移索引值
        for (i in index until size) {
            var offsetIndex = getOffsetIndexByIndex(i)
            offsetIndex += dataBlockSize
            setOffsetIndexByIndex(i, offsetIndex)
        }

        if(index!=size){
            //全部后移4个字节 腾出位置
            move(getOffsetIndexPositionByIndex(index), (size - index) * intByteSize, intByteSize)
        }

        //设置元素的偏移索引
        setOffsetIndexByIndex(index, elementOffsetIndex)
           size += 1
    }

    override fun addAll(index: Int, elements: Collection<E>): Boolean {
        for (element in elements) {
            add(index, element)
        }
        return true
    }

    override fun addAll(elements: Collection<E>): Boolean {
        for (element in elements) {
            //必须重新获取size 保证加入到队尾
            add(size, element)

        }
        return true
    }

    override fun clear() {
        //从尾部移除可以不用移动偏移索引
        for (i in size - 1 downTo 0) {
            removeAt(i)
        }
    }

    override fun remove(element: E): Boolean {
        val index = indexOf(element)

        if (index == -1) {
            return false
        }

        removeAt(index)

        return true
    }

    /**
     * 粗暴的实现
     */
    override fun removeAll(elements: Collection<E>): Boolean {
        for (element in elements) {
            remove(element)
        }
        return true;
    }

    override fun removeAt(index: Int): E {
        if (index >= size || index < 0) {
            throw IndexOutOfBoundsException("size is $size but index is $index")
        }
        //数据块的文件偏移量
        val dataBlockFilePosition = getDataBlockFilePositionByIndex(index)

        val meta = getBlockMetaByIndex(index)
        val element = serializer.fromBytes(getByteArray(meta.dataFilePosition, meta.dataSize))

        //设置删除标记位
        fileMapper.position(dataBlockFilePosition).put(1)


        move(
            //下一个偏移索引的位置
            getOffsetIndexPositionByIndex(index) + intByteSize,
            //要把后面的所有偏移索引向前挪
            //0 0
            (size -1 - index) * intByteSize,
            //移动4个字节
            -1 * intByteSize
        )

        size--
        return element

    }

    private fun printIndexes(){
        fileMapper.position(OFFSET_INDEX_POSITION)
        for(i in 0 until size){
            print("${fileMapper.position(OFFSET_INDEX_POSITION + i * intByteSize).asIntBuffer().get()}  ")
        }
        println()
    }
    /**
     * 粗暴的实现
     */
    override fun retainAll(elements: Collection<E>): Boolean {
        removeAll(elements)
        addAll(elements)
        return true
    }

    override fun set(index: Int, element: E): E {
        removeAt(index)
        add(index, element)
        return element
    }


    override fun iterator(): MutableIterator<E> {
        return listIterator()
    }

    override fun lastIndexOf(element: E): Int {
        return searchIndex(element, size - 1 downTo 0)
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

    fun finishing(){
        var index = 0
        var spaceBytes = 0
        var offset = 0
        //因为数据块是按照顺序的，所以找到最后一个有效元素就行 之后的全部废弃
        while(index<size){
            val meta = getBlockMetaByOffsetIndex(offset)

            val dataBlockLength = meta.dataSize+ DATA_BLOCK_META_LENGTH
            if(meta.delete){
                spaceBytes+=dataBlockLength
            }else{
                //移动数据块
                move(offset + dataAreaPosition,dataBlockLength,-1*spaceBytes)
                //设置新的偏移索引
                setOffsetIndexByIndex(index,offset-spaceBytes)
                index++
            }
            offset+=dataBlockLength
        }


    }
    /**
     * 返回的list不是PersistenceList!!!!!!
     */
    override fun subList(fromIndex: Int, toIndex: Int): MutableList<E> {
        val mutableList = ArrayList<E>()
        for (i in fromIndex until toIndex) {
            mutableList.add(get(i))
        }
        return mutableList
    }

    override fun toString(): String {
        val builder = StringBuilder()
        builder.append("{")

        val iterator = iterator()
        while(iterator.hasNext()){
            builder.append("${iterator.next()}")

            if(iterator.hasNext()){
                builder.append(" ,")
            }
        }
        builder.append("}")

        return builder.toString()
    }
}