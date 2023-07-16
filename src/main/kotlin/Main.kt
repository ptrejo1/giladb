import kotlin.system.measureNanoTime

fun main() {
    wal()
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
//    val wal = WAL.create("/Users/phoenix.trejo/IdeaProjects/giladb/wal")
//    wal.set(WALEntry.create("foo", "{'this': 'is not very fun'}"))
//    wal.set(WALEntry.create("boo", "{'this': 'is fun'}"))
//    wal.flush()

    val wal = WAL.existing("/Users/phoenix.trejo/IdeaProjects/giladb/wal/1425010941684.wal")
    wal.entries().forEach {
        println(it.key.toString(Charsets.UTF_8))
    }
}
