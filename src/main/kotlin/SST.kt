import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.CRC32
import kotlin.io.path.deleteIfExists

class SST private constructor(
    private val filePath: Path,
    val startKey: ByteArray,
    val endKey: ByteArray,
    val index: List<ByteArray>,
    val dataSize: Int
) {

    companion object {

        /** 4 KB */
        const val BLOCK_SIZE = 4000

        private const val KEY_OFFSET = Int.SIZE_BYTES * 3
        private const val ALIVE_INT = 0
        private const val TOMBSTONE_INT = 1

        fun fromMemTable(directory: String, memTable: MemTable): SST {
            // File format
            // <beginning_of_file>
            // block count (Int)
            // [data block 1] 4 KB
            // ...
            // [data block N]
            // [index size (Int)]
            // [index block]
            // -- [key1 size (Int), key1]
            // -- ...
            // -- [keyN size (Int), keyN]
            val filePath = Paths.get(directory, "${System.currentTimeMillis()}.sst")
            Files.createFile(filePath)
            val outputStream = FileOutputStream(filePath.toFile(), true)
            val bufferedWriter = BufferedOutputStream(outputStream)

            // reserve blockCount spot
            val blockCountReserve = ByteArray(Int.SIZE_BYTES)
            bufferedWriter.write(blockCountReserve)

            // write entries
            var runningBlockSize = 0
            var blockCount = 1
            val indexEntries = mutableListOf<ByteArray>()
            var indexSize = 0
            var firstKey: ByteArray = byteArrayOf()
            var endKey: ByteArray = byteArrayOf()
            memTable.entries().forEachIndexed { index, memTableEntry ->
                if (index == 0) firstKey = memTableEntry.key
                if (index == memTable.entriesCount - 1) endKey = memTableEntry.key

                val (encoded, entrySize) = encode(memTableEntry)
                if (runningBlockSize + entrySize > BLOCK_SIZE) {
                    val padding = ByteBuffer.allocate(BLOCK_SIZE - runningBlockSize)
                    bufferedWriter.write(padding.array())
                    blockCount += 1
                    runningBlockSize = 0
                }

                bufferedWriter.write(encoded)
                if (runningBlockSize == 0) {
                    indexEntries.add(toByteArray(memTableEntry.key.size))
                    indexEntries.add(memTableEntry.key)
                    indexSize += (Int.SIZE_BYTES + memTableEntry.key.size)
                }
                runningBlockSize += entrySize
            }
            // pad last block
            val padding = ByteBuffer.allocate(BLOCK_SIZE - runningBlockSize)
            bufferedWriter.write(padding.array())

            // write index
            bufferedWriter.write(toByteArray(indexSize))
            indexEntries.forEach { bufferedWriter.write(it) }
            bufferedWriter.close()

            // write blockCount
            // todo: this seems wrong
            val raf = RandomAccessFile(filePath.toFile(), "rw")
            raf.seek(0)
            raf.write(toByteArray(blockCount))
            raf.close()

            // drop key sizes
            val index = indexEntries.filterIndexed { idx, _ -> idx % 2 != 0 }

            return SST(filePath, firstKey, endKey, index, memTable.arenaOffset)
        }

        fun encode(memTableEntry: MemTableEntry): Pair<ByteArray, Int> {
            // encode format
            // -----------------------------------------------------------------------
            // | ----------------------- header ------------------------ |
            // | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) |
            // -----------------------------------------------------------------------
            val crc = CRC32()
            val tombstoneAsInt = if (memTableEntry.tombstone) TOMBSTONE_INT else ALIVE_INT
            val header = toByteArray(tombstoneAsInt, memTableEntry.key.size, memTableEntry.value.size)
            val checksum = toByteArray(crc.checksum(header, memTableEntry.key, memTableEntry.value))

            val totalSize = (Int.SIZE_BYTES * 3) + memTableEntry.key.size + memTableEntry.value.size + Long.SIZE_BYTES
            val encoded = ByteBuffer.allocate(totalSize)
            encoded.put(0, header)
            encoded.put(KEY_OFFSET, memTableEntry.key)
            encoded.put(KEY_OFFSET + memTableEntry.key.size, memTableEntry.value)
            encoded.put(KEY_OFFSET + memTableEntry.key.size + memTableEntry.value.size, checksum)

            return encoded.array() to encoded.array().size
        }
    }

    fun reloadIndex(): List<ByteArray> {
        // todo: may never need since sst is immutable and index is precomputed
        val inputStream = filePath.toFile().inputStream()
        val sizeBuffer = ByteBuffer.allocate(Int.SIZE_BYTES)
        inputStream.read(sizeBuffer.array())
        val blockCount = sizeBuffer.getInt(0)

        val indexPosition = blockCount * BLOCK_SIZE.toLong()
        // blockCount (Int) + blocks
        inputStream.channel.position(Int.SIZE_BYTES + indexPosition)
        inputStream.channel.read(sizeBuffer)
        val indexSize = sizeBuffer.getInt(0)

        val indexBuffer = ByteBuffer.allocate(indexSize)
        inputStream.channel.read(indexBuffer)
        val index = mutableListOf<ByteArray>()
        indexBuffer.rewind()
        while (indexBuffer.position() < indexSize) {
            val keySize = indexBuffer.getInt(indexBuffer.position())
            indexBuffer.position(indexBuffer.position() + Int.SIZE_BYTES)
            val key = indexBuffer.array().copyOfRange(indexBuffer.position(), indexBuffer.position() + keySize)
            index.add(key)
            indexBuffer.position(indexBuffer.position() + keySize)
        }

        inputStream.close()

        return index
    }

    fun get(key: ByteArray): MemTableEntry? {
        val i = index.binarySearch { GilaByteArray.compare(it, key) }
        val blockIndex = if (i < 0) (-1 * i) - 2 else i

        val inputStream = filePath.toFile().inputStream()
        val position = Int.SIZE_BYTES + (blockIndex * BLOCK_SIZE.toLong())
        inputStream.channel.position(position)
        while (inputStream.channel.position() <= position + BLOCK_SIZE) {
            val entry = decode(inputStream.channel) ?: break
            if (GilaByteArray.compare(key, entry.key) == 0) {
                inputStream.close()
                return entry
            }
        }

        inputStream.close()
        return null
    }

    private fun decode(channel: FileChannel): MemTableEntry? {
        val intBuffer = ByteBuffer.allocate(Int.SIZE_BYTES)
        channel.read(intBuffer)
        val tombstone = intBuffer.getInt(0)

        intBuffer.rewind()
        channel.read(intBuffer)
        val keySize = intBuffer.getInt(0)
        if (keySize == 0) return null

        intBuffer.rewind()
        channel.read(intBuffer)
        val valueSize = intBuffer.getInt(0)

        val keyBuffer = ByteBuffer.allocate(keySize)
        val valueBuffer = ByteBuffer.allocate(valueSize)
        channel.read(keyBuffer)
        channel.read(valueBuffer)

        val crcBuffer = ByteBuffer.allocate(Long.SIZE_BYTES)
        channel.read(crcBuffer)
        val decodedChecksum = crcBuffer.getLong(0)

        val crc = CRC32()
        val header = toByteArray(tombstone, keySize, valueSize)
        if (decodedChecksum != crc.checksum(header, keyBuffer.array(), valueBuffer.array()))
            throw InvalidChecksumException()

        return MemTableEntry(keyBuffer.array(), valueBuffer.array(), tombstone == TOMBSTONE_INT)
    }

    fun delete(): Boolean = filePath.deleteIfExists()
}
