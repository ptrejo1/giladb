import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * byte array representation
 * -----------------------------------------------------------------------
 * | ----------------------- header ------------------------ |
 * | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) | timestamp (Long) |
 * -----------------------------------------------------------------------
 */
class WALEntry(
    private val key: ByteArray,
    private val value: ByteArray,
    private val tombstone: Boolean,
    private val timestamp: Long
) {

    companion object {

        private const val KEY_LENGTH_OFFSET = Int.SIZE_BYTES
        private const val VALUE_LENGTH_OFFSET = Int.SIZE_BYTES * 2
        private const val KEY_OFFSET = Int.SIZE_BYTES * 3
        private const val ALIVE_INT = 0
        private const val TOMBSTONE_INT = 1

        fun decode(buffer: ByteArray): WALEntry {
            // byte array representation
            // -----------------------------------------------------------------------
            // | ----------------------- header ------------------------ |
            // | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) | timestamp (Long) |
            // -----------------------------------------------------------------------
            val wrapped = ByteBuffer.wrap(buffer)
            val decodedTombstone = wrapped.getInt(0)
            val decodedKeyLength = wrapped.getInt(KEY_LENGTH_OFFSET)
            val decodedValueLength = wrapped.getInt(VALUE_LENGTH_OFFSET)

            val valueOffset = KEY_OFFSET + decodedKeyLength
            val decodedKey = buffer.copyOfRange(KEY_OFFSET, valueOffset)
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
    }

    fun encode(): ByteArray {
        // byte array representation
        // -----------------------------------------------------------------------
        // | ----------------------- header ------------------------ |
        // | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) | timestamp (Long) |
        // -----------------------------------------------------------------------
        val crc = CRC32()
        val tombstoneAsInt = if (tombstone) TOMBSTONE_INT else ALIVE_INT
        val header = toByteArray(tombstoneAsInt, key.size, value.size)
        val checksum = toByteArray(crc.checksum(header, key, value))

        val totalSize = (Int.SIZE_BYTES * 3) + key.size + value.size + (Long.SIZE_BYTES * 2)
        val encoded = ByteBuffer.allocate(totalSize)
        encoded.put(0, header)
        encoded.put(KEY_OFFSET, key)
        encoded.put(KEY_OFFSET + key.size, value)
        encoded.put(KEY_OFFSET + key.size + value.size, checksum)
        encoded.put(KEY_OFFSET + key.size + value.size + checksum.size, toByteArray(timestamp))

        return encoded.array()
    }
}