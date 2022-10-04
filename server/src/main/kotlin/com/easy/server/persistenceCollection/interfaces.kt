package com.easy.server.persistenceCollection

interface PersistenceList<E> : MutableList<E>
interface PersistenceSet<E> : MutableSet<E>
interface PersistenceMap<K, V> : MutableMap<K, V>
object FileType {

    val NEW_FILE = 0L
    val ArrayList = 1L
    val MutableSet = 3L
    val MutableMap = 4L

    fun isNewFile(mark: Long) = mark == NEW_FILE

}
enum class FileMapperType{
    MemoryMapMapper,RandomAccessFileMapper,MergedMemoryMapMapper
}

val LongBytesSize = 8
fun<E> retainAll(elements: Collection<E>,iterator:MutableIterator<E>):Boolean{
    var modify = false
    while(iterator.hasNext()){
        val e = iterator.next()
        if(!elements.contains(e)){
            modify = true
            iterator.remove()
        }
    }
    return modify
}
fun <T> allSuccess(elements:Collection<T> ,block:(T)->Boolean):Boolean{
    var success = true
    for (element in elements) {
        success = success and block(element)
    }
    return success
}
fun iterToString(iterator: Iterator<*>):String{
    val builder = StringBuilder()
    builder.append("{")

    while(iterator.hasNext()){
        builder.append("${iterator.next()}")

        if(iterator.hasNext()){
            builder.append(" ,")
        }
    }
    builder.append("}")

    return builder.toString()
}

fun<E> compareCollection (c1: Collection<E>, c2: Collection<E> ):Boolean{
    val iterator = c2.iterator()
    val iterator1 = c1.iterator()
    while(iterator.hasNext()&&iterator1.hasNext()){
        if(iterator.next()!=iterator1.next()){
            return false
        }
    }
    if(iterator.hasNext()||iterator1.hasNext()){
        return false
    }
    return true
}