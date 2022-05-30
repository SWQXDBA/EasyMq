package com.easy.server
open class Father
class Son: Father(){
    var value:String?=null
}
object sonObj{
    val son:Son? = getFather() as? Son
}
fun main() {


    sonObj?.son?.value?.let { println("${it.toString()}") }

}
fun getSon():Son{
    return Son()
}
fun getFather(): Father? {
    return null
}