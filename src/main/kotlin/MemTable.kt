import java.nio.ByteBuffer

class MemTableOverflowException: Exception()

/** 64 MB */
const val MEM_TABLE_DEFAULT_MAX_SIZE = 64 * 1000 * 1000

/**
 * @param maxSize of the memtable in bytes
 */
class MemTable(private val maxSize: Int = MEM_TABLE_DEFAULT_MAX_SIZE) {

    private val arena = ByteBuffer.allocateDirect(maxSize)
    private val index = AVLTree()

    var entriesCount = 0
        private set
    var arenaOffset = 0
        private set

    /**
     * @throws [MemTableOverflowException]
     */
    fun set(entry: MemTableEntry) {
        val encoded = entry.encode()
        if (arenaOffset + encoded.size > maxSize)
            throw MemTableOverflowException()

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

    fun entries() = sequence {
        index.traverse().forEach {
            val (entry, _) = decodeAtOffset(it.offset)
            yield(entry)
        }
    }

    private fun decodeAtOffset(offset: Int): Pair<MemTableEntry, Int> {
        val blockSize = arena.getInt(offset)
        val chunk = ByteArray(blockSize)
        arena.get(offset, chunk)

        return MemTableEntry.decode(chunk) to blockSize
    }
}