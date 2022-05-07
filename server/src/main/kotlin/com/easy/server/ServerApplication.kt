package com.easy.server

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ServerApplication

fun main(args: Array<String>) {
    val application = runApplication<ServerApplication>(*args)
    application.getBean(EasyServer::class.java).run()
}
