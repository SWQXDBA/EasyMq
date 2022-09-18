package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.PersistenceMap

class FilePersistenceMap<K,V>:PersistenceMap<K,V> {
    override val entries: Set<Map.Entry<K, V>>
        get() = TODO("Not yet implemented")
    override val keys: Set<K>
        get() = TODO("Not yet implemented")
    override val size: Int
        get() = TODO("Not yet implemented")
    override val values: Collection<V>
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
}