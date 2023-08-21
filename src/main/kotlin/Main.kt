import java.nio.charset.Charset
import kotlin.system.measureNanoTime

fun main() {
    avl()
}

fun mem() {
    val m = MemTable()
    // warm up
    m.set(MemTableEntry.create("foo", "{'this': 'is not very fun'}"))
    m.get("foo".toByteArray())

    var putTimes = (0 until 1_000_000).map {
        measureNanoTime { m.set(MemTableEntry.create("foo$it", "{'this': 'is not very fun'}")) }
    }

    println("put ${putTimes.average()}")

    putTimes = (0 until 1_000_000).map {
        measureNanoTime { m.get("foo$it".toByteArray()) }
    }

    println("get ${putTimes.average()}")
}

fun wal() {
    val g = GilaDB.open("/Users/phoenix.trejo/gila")
}

fun avl() {
    val a = AVLTree()
    val g = listOf(
        IndexEntry("abc".toByteArray(), 1),
        IndexEntry("zoo".toByteArray(), 1),
        IndexEntry("foo".toByteArray(), 1),
        IndexEntry("abb".toByteArray(), 1),
        IndexEntry("gob".toByteArray(), 1),
    )
    g.forEach { a.insert(it) }

    a.traverse().forEach {
        println(it.key.toString(Charset.defaultCharset()))
    }
}
