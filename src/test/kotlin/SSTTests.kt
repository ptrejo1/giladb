import java.nio.charset.Charset
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.system.measureNanoTime
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
}
