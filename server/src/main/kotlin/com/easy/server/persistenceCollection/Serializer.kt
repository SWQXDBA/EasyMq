package com.easy.server.persistenceCollection


import com.fasterxml.jackson.databind.ObjectMapper
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream


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

class JdkSerializer<T:java.io.Serializable >(
    val type:Class<T>
):Serializer<T>{

    override fun toBytes(obj: T): ByteArray {

        val byteArrayOutputStream = ByteArrayOutputStream()
        val objectOutputStream = ObjectOutputStream(byteArrayOutputStream)
        objectOutputStream.writeObject(obj)
        return  byteArrayOutputStream.toByteArray()
    }


    override fun fromBytes(bytes: ByteArray): T {
        val byteArrayInputStream = ByteArrayInputStream(bytes)
        val objectInputStream = ObjectInputStream(byteArrayInputStream)

        return     objectInputStream.readObject() as T

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