package com.easy.server

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.IntBuffer
import java.nio.channels.FileChannel
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

}
