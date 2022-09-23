package com.easy.server.persistenceCollection


import com.fasterxml.jackson.databind.ObjectMapper


interface Serializer<T>{
    fun toBytes(obj:T):ByteArray
    fun fromBytes(bytes:ByteArray):T
}
object unitSerializer:Serializer<Unit>{

    override fun toBytes(obj: Unit): ByteArray {
        return ByteArray(0)
    }
    override fun fromBytes(bytes: ByteArray): Unit {
        return Unit
    }

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