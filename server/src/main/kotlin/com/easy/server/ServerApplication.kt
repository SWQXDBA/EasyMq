package com.easy.server

import kotlinx.coroutines.runBlocking
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ServerApplication

fun main(args: Array<String>) {
    runBlocking{
        val application = runApplication<ServerApplication>(*args)
        application.getBean(EasyServer::class.java).run()
    }

}
