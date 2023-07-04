import kotlin.system.measureNanoTime

fun main() {

    val m = MemTable()
    // warm up
    m.put(Entry.create("foo", "{'this': 'is not very fun'}"))
    m.get("foo".toByteArray())

    var putTimes = (0 until 1_000_000).map {
        measureNanoTime { m.put(Entry.create("foo", "{'this': 'is not very fun'}")) }
    }

    println("put ${putTimes.average()}")

    putTimes = (0 until 1_000_000).map {
        measureNanoTime { m.get("foo".toByteArray()) }
    }

    println("get ${putTimes.average()}")
}
