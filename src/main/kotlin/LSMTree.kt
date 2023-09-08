import java.nio.file.Files
import java.nio.file.Paths
import java.util.LinkedList
import java.util.Queue
import kotlin.concurrent.thread

class LSMTree private constructor(private val directory: String, private val levels: Int = 3) {

    companion object {

        private val logger by getLogger()

        fun create(directory: String): LSMTree {
            return LSMTree(directory)
        }
    }

    private var memTable = MemTable()
    private val walPath = Paths.get(directory, "wal")
    private var wal: WAL
    private val level0 = Level0()
    private val pendingMemTables: Queue<Pair<MemTable, WAL>> = LinkedList()

    init {
        Files.createDirectories(walPath)
        wal = WAL.create(walPath.toString())

        thread { memTableCompactionLoop() }
    }

    fun get(key: ByteArray): MemTableEntry? {
        memTable.get(key)?.let {
            if (!it.tombstone) return it
        }

        for ((pending, _) in pendingMemTables) {
            pending.get(key)?.let {
                if (!it.tombstone) return it
            }
        }

        level0.get(key)?.let {
            if (!it.tombstone) return it
        }

        return null
    }

    fun set(key: ByteArray, value: ByteArray, tombstone: Boolean) {
        val entry = MemTableEntry(key, value, tombstone)
        val walEntry = WALEntry.create(entry)
        try {
            memTable.set(entry)
            wal.set(walEntry)
            wal.flush()
        } catch (err: MemTableOverflowException) {
            pendingMemTables.add(memTable to wal)
            memTable = MemTable()
            wal = WAL.create(walPath.toString())
            memTable.set(entry)
            wal.set(walEntry)
            wal.flush()
        }
    }

    private fun memTableCompactionLoop() {
        while (true) {
            if (pendingMemTables.isEmpty()) {
                Thread.sleep(1)
                continue
            }

            logger.info("Compacting MemTable")
            val (memTable, wal) = pendingMemTables.peek()
            val sst = SST.fromMemTable(directory, memTable)
            level0.addSST(sst)

            pendingMemTables.remove()
            wal.delete()
        }
    }
}
