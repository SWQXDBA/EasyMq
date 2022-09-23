package com.easy.server.persistenceCollection

interface PersistenceList<E> : MutableList<E>
interface PersistenceSet<E> : Set<E>
interface PersistenceMap<K, V> : MutableMap<K, V>
object FileType {

    val NEW_FILE = 0L
    val ArrayList = 1L
    val MutableSet = 3L
    val MutableMap = 4L
    fun isNewFile(mark: Long) = mark == NEW_FILE

}

val LongBytesSize = 16