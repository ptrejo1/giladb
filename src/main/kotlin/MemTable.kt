import java.nio.ByteBuffer

class TableOverflowException: Exception()

const val MEM_TABLE_DEFAULT_MAX_SIZE = 64 * 1000 * 1000

/**
 * @param maxSize of the memtable in bytes
 */
class MemTable(private val maxSize: Int = MEM_TABLE_DEFAULT_MAX_SIZE) {

    private val arena = ByteBuffer.allocateDirect(maxSize)
    private val index = AVLTree()

    private var entriesCount = 0
    private var arenaOffset = 0

    /**
     * @throws [TableOverflowException]
     */
    fun put(entry: MemTableEntry) {
        val encoded = entry.encode()
        if (arenaOffset + encoded.size > maxSize)
            throw TableOverflowException()

        index.insert(IndexEntry(entry.key, arenaOffset))
        arena.put(arenaOffset, encoded)
        entriesCount += 1
        arenaOffset += encoded.size
    }

    fun get(key: ByteArray): MemTableEntry? {
        val indexEntry = index.search(key, ComparisonType.LTE) ?: return null
        val (entry, _) = decodeAtOffset(indexEntry.offset)

        return entry
    }

    fun scan() = sequence {
        var runningOffset = 0
        while (runningOffset < arenaOffset) {
            val (entry, bytesRead) = decodeAtOffset(runningOffset)
            yield(entry)
            runningOffset += bytesRead
        }
    }

    private fun decodeAtOffset(offset: Int): Pair<MemTableEntry, Int> {
        val blockSize = arena.getInt(offset)
        // size of blockSize + size of block
        val totalBlockSize = Int.SIZE_BYTES + blockSize
        val chunk = ByteArray(totalBlockSize)
        arena.get(offset, chunk)

        return MemTableEntry.decode(chunk) to totalBlockSize
    }
}