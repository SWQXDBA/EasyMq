package com.easy.server.persistenceCollection

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.TypeFactory

interface Serializer<T>{
    fun toBytes(obj:T):ByteArray
    fun fromBytes(bytes:ByteArray):T
}
class JacksonSerializer<T>(
    val type:Class<T>
):Serializer<T>{
    val objectMapper=ObjectMapper()
    override fun toBytes(obj: T): ByteArray {
      return  objectMapper.writeValueAsBytes(obj)
    }

    override fun fromBytes(bytes: ByteArray): T {
        return objectMapper.readValue(bytes, type)

    }


}