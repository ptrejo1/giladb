import kotlin.system.measureNanoTime

fun main() {
    val m = MemTable()
    // warm up
    m.put(MemTableEntry.create("foo", "{'this': 'is not very fun'}"))
    m.get("foo".toByteArray())

    var putTimes = (0 until 1_000_000).map {
        measureNanoTime { m.put(MemTableEntry.create("foo", "{'this': 'is not very fun'}")) }
    }

    println("put ${putTimes.average()}")

    putTimes = (0 until 1_000_000).map {
        measureNanoTime { m.get("foo".toByteArray()) }
    }

    println("get ${putTimes.average()}")


    val en = (0 until 1_000_000).map {
        measureNanoTime { WALEntry.create("foo", "{'this': 'is not very fun'}").encode() }
    }
    val v = WALEntry.create("foo", "{'this': 'is not very fun'}").encode()
    val u = WALEntry.decode(v)

    println(en.average())
}
