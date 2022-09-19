package com.easy.server

import com.easy.server.persistenceCollection.JacksonSerializer
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceList
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.util.StopWatch
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.IntBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption

@SpringBootTest
class ServerApplicationTests {

    @Test
    fun fileTest() {

        var size = 1024L;
        var map = FileChannel.open(Path.of("./test.txt"), StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ).map(FileChannel.MapMode.READ_WRITE,0,size)

        var current = 0;
        for(i in 0..1024){
            map.position(current)
            map.asIntBuffer().put(i)
            current+=4
            if(current>=size){
                size*=2
                map = FileChannel.open(Path.of("./test.txt"), StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ).map(FileChannel.MapMode.READ_WRITE,0,size)
            }
        }
        size = 8
        map = FileChannel.open(Path.of("./test.txt"), StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ).map(FileChannel.MapMode.READ_WRITE,0,size)

        current = 0;
        for(i in 0..128){
            map.position(current)
            map.asIntBuffer().put(-i)
            current+=4
            if(current>=size){
                size*=2
                map = FileChannel.open(Path.of("./test.txt"), StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ).map(FileChannel.MapMode.READ_WRITE,0,size)
            }
        }

        current = 0;
        for(i in 0..1024){
            map.position(current)
            println(map.asIntBuffer().get())
            current+=4
            if(current>=size){
                size*=2
                map = FileChannel.open(Path.of("./test.txt"), StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ).map(FileChannel.MapMode.READ_WRITE,0,size)
            }
        }
    }

     class Data() {
         constructor(  str:String?,
                       age:Int?) : this() {
             this.str = str
             this.age = age

         }
         var str:String?=null
         var age:Int?=null
         override fun toString(): String {
             return "Data(str=$str, age=$age)"
         }

         override fun equals(other: Any?): Boolean {
             if (this === other) return true
             if (other !is Data) return false

             if (str != other.str) return false
             if (age != other.age) return false

             return true
         }

         override fun hashCode(): Int {
             var result = str?.hashCode() ?: 0
             result = 31 * result + (age ?: 0)
             return result
         }


     }
    @Test
    fun fileTest2() {
        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceList("./test2.txt",JacksonSerializer(Data::class.java))
        for (i in 0..3){
            persistenceList.add(Data("str:: $i",i))
        }
        println(persistenceList)

        persistenceList.remove(Data("str:: 2",2))
        println(persistenceList)



    }
    @Test
    fun fileTest3() {
        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceList("./test2.txt",JacksonSerializer(Data::class.java))
        for (i in 0..10000000){
            persistenceList.add(Data("str:: $i",i))
        }

        println(persistenceList.usageFileSize)


        for (i in 0..955){
            persistenceList.removeAt(0)
        }
        var stopWatch=StopWatch()
        stopWatch.start()
        println(persistenceList.usageFileSize)

        persistenceList.finishing()
        println(persistenceList.usageFileSize)
        stopWatch.stop()
        println(stopWatch.totalTimeMillis)
    }
    @Test
    fun fileTest4() {
        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceList("./test2.txt",JacksonSerializer(Data::class.java))
        persistenceList.add(Data("str:: /",1))
        for (i in 0..100){
            var stopWatch=StopWatch()
            stopWatch.start()
            for (j in 0..1000){
                persistenceList.add(1,Data("str:: $i $j",i+j))
            }
            stopWatch.stop()
            println(stopWatch.totalTimeMillis)
        }


    }
}
