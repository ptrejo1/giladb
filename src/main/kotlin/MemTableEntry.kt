import java.nio.ByteBuffer
import java.util.zip.CRC32

class InvalidChecksumException: Exception()

/**
 * byte array representation
 * -----------------------------------------------------------------------
 *                    | ----------------------- header ------------------------ |
 * | block size (Int) | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) |
 * -----------------------------------------------------------------------
 */
class MemTableEntry(val key: ByteArray, private val value: ByteArray, private val tombstone: Boolean) {

    companion object {

        private const val TOMBSTONE_OFFSET = Int.SIZE_BYTES
        private const val KEY_LENGTH_OFFSET = Int.SIZE_BYTES * 2
        private const val VALUE_LENGTH_OFFSET = Int.SIZE_BYTES * 3
        private const val KEY_OFFSET = Int.SIZE_BYTES * 4
        private const val ALIVE_INT = 0
        private const val TOMBSTONE_INT = 1

        /**
         * @throws [InvalidChecksumException]
         */
        fun decode(buffer: ByteArray): MemTableEntry {
            // byte array representation
            // -----------------------------------------------------------------------
            //                    | ----------------------- header ------------------------ |
            // | block size (Int) | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) |
            // -----------------------------------------------------------------------
            val wrapped = ByteBuffer.wrap(buffer)
            val decodedTombstone= wrapped.getInt(TOMBSTONE_OFFSET)
            val decodedKeyLength = wrapped.getInt(KEY_LENGTH_OFFSET)
            val decodedValueLength = wrapped.getInt(VALUE_LENGTH_OFFSET)

            val valueOffset = KEY_OFFSET + decodedKeyLength
            val decodedKey = buffer.copyOfRange(KEY_OFFSET, valueOffset)
            val valueEnd = valueOffset + decodedValueLength
            val decodedValue = buffer.copyOfRange(valueOffset, valueEnd)
            val decodedChecksum = wrapped.getLong(valueEnd)

            val crc = CRC32()
            val decodedHeader = toByteArray(decodedTombstone, decodedKey.size, decodedValue.size)
            if (decodedChecksum != crc.checksum(decodedHeader, decodedKey, decodedValue))
                throw InvalidChecksumException()

            return MemTableEntry(decodedKey, decodedValue, decodedTombstone == TOMBSTONE_INT)
        }

        fun create(key: String, value: String, tombstone: Boolean = false): MemTableEntry =
            MemTableEntry(key.toByteArray(), value.toByteArray(), tombstone)
    }

    fun encode(): ByteArray {
        // byte array representation
        // -----------------------------------------------------------------------
        //                    | ----------------------- header ------------------------ |
        // | block size (Int) | tombstone (Int) | key length (Int) | value length (Int) | key | value | crc32 (Long) |
        // -----------------------------------------------------------------------
        val crc = CRC32()
        val tombstoneAsInt = if (tombstone) TOMBSTONE_INT else ALIVE_INT
        val header = toByteArray(tombstoneAsInt, key.size, value.size)
        val checksum = toByteArray(crc.checksum(header, key, value))

        val totalSize = (Int.SIZE_BYTES * 4) + key.size + value.size + Long.SIZE_BYTES
        val encoded = ByteBuffer.allocate(totalSize)
        encoded.put(0, toByteArray(totalSize))
        encoded.put(TOMBSTONE_OFFSET, header)
        encoded.put(KEY_OFFSET, key)
        encoded.put(KEY_OFFSET + key.size, value)
        encoded.put(KEY_OFFSET + key.size + value.size, checksum)

        return encoded.array()
    }
}
