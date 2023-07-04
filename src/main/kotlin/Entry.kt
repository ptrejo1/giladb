import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.zip.CRC32

class ChecksumValidationException: Exception()

/**
 * byte array representation of Entry, where crc32 is the checksum of header and k/v
 * -----------------------------------------------------------------------
 * | block size (Int) | header | key | value | crc32 (Long) |
 *                        where header is
 * | tombstone (Int) | key length (Int) | value length (Int) |
 * -----------------------------------------------------------------------
 */
class Entry(val key: ByteArray, private val value: ByteArray, private val tombstone: Boolean) {

    companion object {

        private const val TOMBSTONE_OFFSET = 4
        private const val KEY_LENGTH_OFFSET = 8
        private const val VALUE_LENGTH_OFFSET = 12
        private const val KEY_OFFSET = 16
        private const val ALIVE_INT = 0
        private const val TOMBSTONE_INT = 1

        /**
         * @throws [ChecksumValidationException]
         */
        fun decode(buffer: ByteArray): Entry {
            // byte array representation of Entry, where crc32 is the checksum of header and k/v
            // -----------------------------------------------------------------------
            // | block size (Int) | header | key | value | crc32 (Long) |
            //                        where header is
            // | tombstone (Int) | key length (Int) | value length (Int) |
            // -----------------------------------------------------------------------
            val wrapped = ByteBuffer.wrap(buffer)
            val decodedTombstone= wrapped.getInt(TOMBSTONE_OFFSET)
            val decodedKeyLength = wrapped.getInt(KEY_LENGTH_OFFSET)
            val decodedValueLength = wrapped.getInt(VALUE_LENGTH_OFFSET)

            val valueOffset = KEY_OFFSET + decodedKeyLength
            val decodedKey = buffer.copyOfRange(KEY_OFFSET, valueOffset)
            val valueEnd = valueOffset + decodedValueLength
            val decodedValue = buffer.copyOfRange(valueOffset, valueEnd)
            val checksum = wrapped.getLong(valueEnd)

            val crc = CRC32()
            val header = toByteArray(decodedTombstone, decodedKey.size, decodedValue.size)
            crc.update(header)
            crc.update(decodedKey)
            crc.update(decodedValue)

            if (checksum != crc.value)
                throw ChecksumValidationException()

            val tombstone = decodedTombstone == TOMBSTONE_INT
            return Entry(decodedKey, decodedValue, tombstone)
        }

        fun create(key: String, value: String, tombstone: Boolean = false): Entry {
            return Entry(key.toByteArray(), value.toByteArray(), tombstone)
        }
    }

    // TODO: add compression
    fun encode(): ByteArray {
        // byte array representation of Entry, where crc32 is the checksum of header and k/v
        // -----------------------------------------------------------------------
        // | block size (Int) | header | key | value | crc32 (Long) |
        //                        where header is
        // | tombstone (Int) | key length (Int) | value length (Int) |
        // -----------------------------------------------------------------------
        val crc = CRC32()
        val data = ByteArrayOutputStream()
        val tombstoneAsInt = if (tombstone) TOMBSTONE_INT else ALIVE_INT

        val header = toByteArray(tombstoneAsInt, key.size, value.size)
        crc.update(header)
        data.write(header)

        crc.update(key)
        data.write(key)

        crc.update(value)
        data.write(value)

        val checksum = toByteArray(crc.value)
        data.write(checksum)

        val encoded = ByteArrayOutputStream()
        encoded.write(toByteArray(data.size()))
        encoded.write(data.toByteArray())

        return encoded.toByteArray()
    }
}
