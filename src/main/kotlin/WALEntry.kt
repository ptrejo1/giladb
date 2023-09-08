import java.nio.ByteBuffer
import java.util.zip.CRC32

class WALEntry(
    val key: ByteArray,
    val value: ByteArray,
    val tombstone: Boolean,
    val timestamp: Long
) {

    companion object {

        private const val TOMBSTONE_ENCODE_OFFSET = Int.SIZE_BYTES
        private const val KEY_ENCODE_OFFSET = Int.SIZE_BYTES * 4
        private const val KEY_LENGTH_DECODE_OFFSET = Int.SIZE_BYTES
        private const val VALUE_LENGTH_DECODE_OFFSET = Int.SIZE_BYTES * 2
        private const val KEY_DECODE_OFFSET = Int.SIZE_BYTES * 3
        private const val ALIVE_INT = 0
        private const val TOMBSTONE_INT = 1

        fun decode(buffer: ByteArray): WALEntry {
            // decode format
            // -----------------------------------------------------------------------
            // | ----------------------- header ------------------------ |
            // | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) | timestamp (Long) |
            // -----------------------------------------------------------------------
            val wrapped = ByteBuffer.wrap(buffer)
            val decodedTombstone = wrapped.getInt(0)
            val decodedKeyLength = wrapped.getInt(KEY_LENGTH_DECODE_OFFSET)
            val decodedValueLength = wrapped.getInt(VALUE_LENGTH_DECODE_OFFSET)

            val valueOffset = KEY_DECODE_OFFSET + decodedKeyLength
            val decodedKey = buffer.copyOfRange(KEY_DECODE_OFFSET, valueOffset)
            val valueEnd = valueOffset + decodedValueLength
            val decodedValue = buffer.copyOfRange(valueOffset, valueEnd)
            val decodedChecksum = wrapped.getLong(valueEnd)
            val decodedTimestamp = wrapped.getLong(valueEnd + Long.SIZE_BYTES)

            val crc = CRC32()
            val decodedHeader = toByteArray(decodedTombstone, decodedKey.size, decodedValue.size)
            if (decodedChecksum != crc.checksum(decodedHeader, decodedKey, decodedValue))
                throw InvalidChecksumException()

            return WALEntry(decodedKey, decodedValue, decodedTombstone == TOMBSTONE_INT, decodedTimestamp)
        }

        fun create(key: String, value: String, tombstone: Boolean = false): WALEntry =
            WALEntry(key.toByteArray(), value.toByteArray(), tombstone, System.currentTimeMillis())

        fun create(memTableEntry: MemTableEntry): WALEntry =
            WALEntry(memTableEntry.key, memTableEntry.value, memTableEntry.tombstone, System.nanoTime())
    }

    fun encode(): ByteArray {
        // encode format, where block size doesn't include itself
        // -----------------------------------------------------------------------
        //                    | ----------------------- header ------------------------ |
        // | block size (Int) | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) | timestamp (Long) |
        // -----------------------------------------------------------------------
        val crc = CRC32()
        val tombstoneAsInt = if (tombstone) TOMBSTONE_INT else ALIVE_INT
        val header = toByteArray(tombstoneAsInt, key.size, value.size)
        val checksum = toByteArray(crc.checksum(header, key, value))

        val totalSize = (Int.SIZE_BYTES * 4) + key.size + value.size + (Long.SIZE_BYTES * 2)
        val encoded = ByteBuffer.allocate(totalSize)
        encoded.put(0, toByteArray(totalSize - Int.SIZE_BYTES))
        encoded.put(TOMBSTONE_ENCODE_OFFSET, header)
        encoded.put(KEY_ENCODE_OFFSET, key)
        encoded.put(KEY_ENCODE_OFFSET + key.size, value)
        encoded.put(KEY_ENCODE_OFFSET + key.size + value.size, checksum)
        encoded.put(KEY_ENCODE_OFFSET + key.size + value.size + checksum.size, toByteArray(timestamp))

        return encoded.array()
    }
}