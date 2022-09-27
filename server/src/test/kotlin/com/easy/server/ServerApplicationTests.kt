package com.easy.server

import com.easy.server.persistenceCollection.JacksonSerializer
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceArrayList
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceList
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceMap
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceSet
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.util.StopWatch
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*

@SpringBootTest
class ServerApplicationTests {

    @Test
    fun fileTest() {

        var size = 1024L;
        var map = FileChannel.open(
            Path.of("./test.txt"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ
        ).map(FileChannel.MapMode.READ_WRITE, 0, size)

        var current = 0;
        for (i in 0..1024) {
            map.position(current)
            map.asIntBuffer().put(i)
            current += 4
            if (current >= size) {
                size *= 2
                map = FileChannel.open(
                    Path.of("./test.txt"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ
                ).map(FileChannel.MapMode.READ_WRITE, 0, size)
            }
        }
        size = 8
        map = FileChannel.open(
            Path.of("./test.txt"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ
        ).map(FileChannel.MapMode.READ_WRITE, 0, size)

        current = 0;
        for (i in 0..128) {
            map.position(current)
            map.asIntBuffer().put(-i)
            current += 4
            if (current >= size) {
                size *= 2
                map = FileChannel.open(
                    Path.of("./test.txt"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ
                ).map(FileChannel.MapMode.READ_WRITE, 0, size)
            }
        }

        current = 0;
        for (i in 0..1024) {
            map.position(current)
            println(map.asIntBuffer().get())
            current += 4
            if (current >= size) {
                size *= 2
                map = FileChannel.open(
                    Path.of("./test.txt"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ
                ).map(FileChannel.MapMode.READ_WRITE, 0, size)
            }
        }
    }

    class Data() {
        constructor(
            str: String?,
            age: Int?
        ) : this() {
            this.str = str
            this.age = age

        }

        var str: String? = null
        var age: Int? = null
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
        val persistenceList = FilePersistenceList("./test2.txt", JacksonSerializer(Data::class.java))
        for (i in 0..3) {
            persistenceList.add(Data("str:: $i", i))
        }
        println(persistenceList)

        persistenceList.remove(Data("str:: 2", 2))
        println(persistenceList)


    }

    @Test
    fun fileTest3() {
        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceList("./test2.txt", JacksonSerializer(Data::class.java))
        for (i in 0..10000000) {
            persistenceList.add(Data("str:: $i", i))
        }

        println(persistenceList.usageFileSize)


        for (i in 0..955) {
            persistenceList.removeAt(0)
        }
        var stopWatch = StopWatch()
        stopWatch.start()
        println(persistenceList.usageFileSize)

        persistenceList.compress()
        println(persistenceList.usageFileSize)
        stopWatch.stop()
        println(stopWatch.totalTimeMillis)
    }

    @Test
    fun fileTest4() {
        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceList("./test2.txt", JacksonSerializer(Data::class.java))
        persistenceList.add(Data("str:: /", 1))
        for (i in 0..100) {
            var stopWatch = StopWatch()
            stopWatch.start()
            for (j in 0..1000) {
                persistenceList.add(1, Data("str:: $i $j", i + j))
            }
            stopWatch.stop()
            println(stopWatch.totalTimeMillis)
        }


    }

    @Test
    fun fileTest5() {

        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceList("./test2.txt", JacksonSerializer(Data::class.java))
        persistenceList.add(Data("str:: /", 1))
        var stopWatch = StopWatch()
        stopWatch.start()
        for (i in 0..100) {
            for (j in 0..1000) {
                persistenceList.add(Data("str:: $i $j", i + j))
            }
        }
        stopWatch.stop()
        println(stopWatch.totalTimeMillis)
    }

    @Test
    fun fileTest6() {

        Files.delete(Path.of("./test2.txt"))
        val randomAccessFile = RandomAccessFile("./test2.txt", "rw")
        randomAccessFile.setLength(1024)
        randomAccessFile.close()
        println(Arrays.toString(Files.readAllBytes(Path.of("./test2.txt"))))
    }

    @Test
    fun fileTest7() {

        try {
            Files.delete(Path.of("./test2.txt"))
        } catch (ignore: Exception) {

        }
        val map: FilePersistenceMap<String, Int> = FilePersistenceMap(
            "./test2.txt",
            JacksonSerializer(String::class.java), JacksonSerializer(Int::class.java)
        )
        map["123"] = 123
        map["123"] = 456

        println("map.containsKey(\"12\") :${map.containsKey("12")}")
        println("map.containsKey(\"123\") :${map.containsKey("123")}")
        println("map.containsValue(123) :${map.containsValue(123)}")
        println("map.containsValue(456) :${map.containsValue(456)}")

        map.forEach { t, u ->
            println("$t : $u")
        }


        println("remove 12 ${map.remove("12")} ")
        map.forEach { t, u ->
            println("$t : $u")
        }
        println("remove 123 ${map.remove("123")} ")
        map.forEach { t, u ->
            println("$t : $u")
        }

        for (i in 0..10000) {
            map["$i"] = i
        }
        println("size ${map.size}")
        println(map["50"])
        val stopWatch = StopWatch()
        stopWatch.start()
        println("start...")
        map.clear()
        stopWatch.stop()
        println("time: ${stopWatch.totalTimeMillis}")
        println("clear over")
        println(map)

        stopWatch.start()
        println("start...")


        println("file usageFileSize ${map.usageFileSize}")
        println("fileSize ${map.fileSize}")
        println("compress size is ${map.compress()}")
        println("file usageFileSize after compress is ${map.usageFileSize}")

        stopWatch.stop()
        println("compress use ${stopWatch.totalTimeMillis}")


    }

    @Test
    fun fileTest8() {
        try {
            Files.delete(Path.of("./test2.txt"))
        } catch (ignore: Exception) {

        }
        val map: FilePersistenceMap<String, Int> = FilePersistenceMap(
            "./test2.txt",
            JacksonSerializer(String::class.java), JacksonSerializer(Int::class.java)
        )


        for (i in 0..40) {
            map["$i"] = i
        }
        println(map)
        for (i in 5..39) {
            map.remove("$i")
        }
        println(map)
        println("bef")
        map.compress()
        println(map)
    }

    @Test
    fun fileTest9() {
        try {
            Files.delete(Path.of("./test2.txt"))
        } catch (ignore: Exception) {

        }
        val set: FilePersistenceSet<String> = FilePersistenceSet(
            "./test2.txt",
            JacksonSerializer(String::class.java),
        )

        println(set.isEmpty())
        for (i in 0..3) {
            set.add("$i")
        }
        for (i in 0..3) {
            set.add("$i")
        }
        for (i in 0..3) {
            set.add("$i")
        }
        println(set.toString())
        println(set.size)
        println(set.containsAll(arrayListOf("1", "2")))
        println(set.contains("1"))
        println(set.contains("5"))

        set.retainAll(arrayListOf("1", "2"))
        println(set)
        set.retainAll(arrayListOf("1", "5"))
        println(set)
        for (i in 0..3) {
            set.add("$i")
        }
        println(set)
        val iterator = set.iterator()

        iterator.next()
        println("remove ${iterator.next()} ")
        iterator.remove()
        println(set)

    }


    @Test
    fun fileTest10() {

        Files.delete(Path.of("./test2.txt"))
        val persistenceList = FilePersistenceArrayList("./test2.txt", JacksonSerializer(Data::class.java))

        for (i in 0..18) {
            persistenceList.add(Data("str:: $i", i))
        }
        println(persistenceList)
        for (i in 0..3) {
            persistenceList.remove(Data("str:: $i", i))
        }
        for (i in 7..9) {
            persistenceList.remove(Data("str:: $i", i))
        }
        println(persistenceList)
        println(persistenceList.size)
        println(persistenceList.usageFileSize)
        println("com ${persistenceList.compress()}")
        println(persistenceList.usageFileSize)
        println(persistenceList)

    }
}
