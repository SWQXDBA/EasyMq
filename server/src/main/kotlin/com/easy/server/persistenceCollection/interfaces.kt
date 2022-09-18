package com.easy.server.persistenceCollection

interface PersistenceList<E> :List<E>
interface PersistenceSet<E> :Set<E>
interface PersistenceMap<K,out V> :Map<K,V>