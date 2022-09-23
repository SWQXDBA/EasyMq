package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.PersistenceSet
import com.easy.server.persistenceCollection.Serializer
import com.easy.server.persistenceCollection.retainAll
import com.easy.server.persistenceCollection.unitSerializer

class FilePersistenceSet<E>(
    filePath: String,
    serializer: Serializer<E>,
    autoForceMills: Long = 10,
    forcePerOption: Boolean = false,
    initCap: Int = 16
) :
    PersistenceSet<E> {

    val map = FilePersistenceMap(filePath, serializer, unitSerializer, autoForceMills, forcePerOption, initCap)
    override fun add(element: E): Boolean {
        val contains = map.containsKey(element)
        map[element] = Unit
        return !contains
    }

    override fun remove(element: E): Boolean {
        TODO("Not yet implemented")
    }

    override fun addAll(elements: Collection<E>): Boolean {
        var success = true
        elements.forEach { success = success and add(it) }
        return success
    }

    override fun iterator(): MutableIterator<E> {
        return object : MutableIterator<E> {
            val iterator = map.iterator()
            override fun hasNext(): Boolean {
                return iterator.hasNext()
            }

            override fun next(): E {
                return iterator.next().key
            }

            override fun remove() {
                iterator.remove()
            }
        }
    }


    override fun removeAll(elements: Collection<E>): Boolean {
        var removeAny = false
        elements.forEach {
            removeAny = removeAny or (map.remove(it) != null)
        }
        return removeAny
    }

    override fun retainAll(elements: Collection<E>): Boolean {
        return retainAll(elements, iterator())
    }

    override val size: Int
        get() = map.size

    override fun contains(element: E): Boolean {
        return map.containsKey(element)
    }

    override fun containsAll(elements: Collection<E>): Boolean {
        var containsAll = true
        elements.forEach { containsAll = containsAll and map.containsKey(it) }

        return containsAll
    }

    override fun isEmpty(): Boolean {
        return map.isEmpty()
    }

    override fun clear() {
        map.clear()
    }


}