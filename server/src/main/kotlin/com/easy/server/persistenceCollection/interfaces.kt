package com.easy.server.persistenceCollection

interface PersistenceList<E> :MutableList<E>
interface PersistenceSet<E> :Set<E>
interface PersistenceMap<K,out V> :Map<K,V>