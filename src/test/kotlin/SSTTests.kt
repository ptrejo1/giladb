import java.nio.charset.Charset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class SSTTests {

    private val testDirectory = "/Users/phoenix.trejo/gila/testing"

    private fun setUpMemTable(): Pair<MemTable, Int> {
        val memTable = MemTable()
        repeat(100) {
            memTable.set(MemTableEntry.create(
                randomString(keyRange.first, keyRange.second),
                randomString(valueRange.first, valueRange.second)
            ))
        }
        val sstEncodedSize = memTable.entries().fold(0) { acc, next ->
            acc + SST.encode(next).second
        }

        return memTable to sstEncodedSize
    }

    @Test fun testIndex() {
        val(memTable, _) = setUpMemTable()
        val sst = SST.fromMemTable(testDirectory, memTable)
        val actual = "sst.index[3]".encodeToByteArray()
        println(actual.toString(Charset.defaultCharset()))
        sst.get(actual)
    }

    @Test fun testReload() {
        val(memTable, _) = setUpMemTable()
        val sst = SST.fromMemTable(testDirectory, memTable)
        val r = sst.reloadIndex()
        r.forEach { println(it.toString(Charset.defaultCharset())) }
    }

    @Test fun testEn() {
        val memTable = MemTable()
        val items = (0..<100).map {
            MemTableEntry.create(
                randomString(keyRange.first, keyRange.second),
                randomString(valueRange.first, valueRange.second)
            )
        }
        items.forEach { memTable.set(it) }

        val sst = SST.fromMemTable(testDirectory, memTable)
        val shuffled = items.shuffled()
        for (item in shuffled) {
            val ret = sst.get(item.key)
            assertNotNull(ret)
            assertEquals(ret.tombstone, item.tombstone)
            assertEquals(GilaByteArray.compare(ret.key, item.key), 0)
            assertEquals(GilaByteArray.compare(ret.value, item.value), 0)
        }

        val dne = sst.get("foobar".encodeToByteArray())
        assertNull(dne)
    }

    @Test fun testGila() {
        val gila = GilaDB.open(testDirectory)
        var size = 0
        val sets = mutableListOf<Long>()
        while (size < MEM_TABLE_DEFAULT_MAX_SIZE * 1.5) {
            val key = randomString(keyRange.first, keyRange.second)
            val value = randomString(valueRange.first, valueRange.second)
            val s = System.nanoTime()
            gila.set(key, value)
            val e = System.nanoTime()
            sets.add(e - s)

            size += key.length
            size += value.length
        }

        println(sets.size)
        println(sets.sum())
        println(sets.sum() / sets.size)
    }

    @Test fun testGilaGet() {
        val gila = GilaDB.open(testDirectory)

        gila.set("foobar", "crap")
        var r = gila.get("foobar")
        assertNotNull(r)
        assertEquals(r.key.decodeToString(), "foobar")
        gila.delete("foobar")
        r = gila.get("foobar")
        assertNull(r)

        var size = 0
        val keys = mutableListOf<Pair<String, String>>()
        while (size < MEM_TABLE_DEFAULT_MAX_SIZE * 1.5) {
            val key = randomString(keyRange.first, keyRange.second)
            val value = randomString(valueRange.first, valueRange.second)
            gila.set(key, value)
            size += key.length
            size += value.length
            keys.add(key to value)
        }

        val times = mutableListOf<Long>()
        keys.shuffle()
        keys.forEachIndexed { idx, (k, v) ->
            val s = System.nanoTime()
            val ret = gila.get(k)
            val e = System.nanoTime()
            assertNotNull(ret)
            assertEquals(ret.value.decodeToString(), v)
            times.add(e - s)
        }

        println(times.size)
        println(times.sum())
        println(times.sum() / times.size)
    }
}
