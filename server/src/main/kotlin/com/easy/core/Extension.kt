package com.easy.core

import java.util.concurrent.ConcurrentHashMap

public fun<K,V> ConcurrentHashMap<K,V>.putIfAbsent(key:K,block:()->V){
    if(contains(key)){
        return;
    }
    val value :V = block()
    putIfAbsent(key,value);
}