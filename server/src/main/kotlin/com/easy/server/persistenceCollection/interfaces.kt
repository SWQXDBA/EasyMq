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