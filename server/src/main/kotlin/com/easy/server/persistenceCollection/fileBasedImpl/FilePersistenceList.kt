package com.easy.server.persistenceCollection.fileBasedImpl

import com.easy.server.persistenceCollection.PersistenceList
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

class FilePersistenceList<E>(
    val filePath:String
):PersistenceList<E> {

    init {
        val path = Path.of(filePath)
        if(path.exists()){

        }else{
//            Files.createFile(filePath)
        }
    }
    override val size: Int
        get() = TODO("Not yet implemented")

    override fun contains(element: E): Boolean {
        TODO("Not yet implemented")
    }

    override fun containsAll(elements: Collection<E>): Boolean {
        TODO("Not yet implemented")
    }

    override fun get(index: Int): E {
        TODO("Not yet implemented")
    }

    override fun indexOf(element: E): Int {
        TODO("Not yet implemented")
    }

    override fun isEmpty(): Boolean {
        TODO("Not yet implemented")
    }

    override fun iterator(): Iterator<E> {
        TODO("Not yet implemented")
    }

    override fun lastIndexOf(element: E): Int {
        TODO("Not yet implemented")
    }

    override fun listIterator(): ListIterator<E> {
        TODO("Not yet implemented")
    }

    override fun listIterator(index: Int): ListIterator<E> {
        TODO("Not yet implemented")
    }

    override fun subList(fromIndex: Int, toIndex: Int): List<E> {
        TODO("Not yet implemented")
    }
}