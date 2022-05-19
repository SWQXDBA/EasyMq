package com.easy.server

import com.easy.EasyClient
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.delay
import java.lang.reflect.Field
import kotlin.coroutines.*
import kotlin.properties.Delegates
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.javaField

class SqlBuilder(val sql: String) {

    infix fun select(other: Field): SqlBuilder {
        return SqlBuilder("select " + other.name)
    }

    infix fun <T> from(other: Class<T>): SqlBuilder {
        return SqlBuilder(sql + " from " + other.simpleName)
    }

    infix fun where(other: Field): SqlBuilder {
        return SqlBuilder(sql + " where " + other.name)
    }

    infix fun than(other: Field): SqlBuilder {
        return SqlBuilder(sql + " where " + other.name)
    }

    fun show() {
        println(sql)
    }
}

class Entity(val name: String) {
    companion object{
       val c = "123"
        fun doSth(){
            println("dosth")
        }
    }
    var age by Delegates.notNull<Int>();

    init {
        age = 15
    }
}

fun x() {

}

fun <T> coroutine(block: suspend () -> T) {
    block.createCoroutine(object : Continuation<T> {
        override val context: CoroutineContext
            get() = EmptyCoroutineContext

        override fun resumeWith(result: Result<T>) {
        }

    }).resume(Unit)
}
object Obj{

}

fun main() {

    println(Entity.c)
    Entity.doSth()
    println(Obj)
    coroutine {
        println("ok1" + Thread.currentThread().name)
        delay(200)
        println("ok1" + Thread.currentThread().name)
    }

    coroutine {
        println("ok2" + Thread.currentThread().name)
        delay(200)
        println("ok2" + Thread.currentThread().name)
    }
    val f1 = fun(): Int {
        println("f1")
        return 1
    }

    (SqlBuilder("") select Entity::name.javaField!! from Entity::class.java where Entity::name.javaField!!).show()
}