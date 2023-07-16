import java.nio.ByteBuffer
import java.util.zip.CRC32

fun toByteArray(vararg items: Int): ByteArray {
    val buffer = ByteBuffer.allocate(Int.SIZE_BYTES * items.size)
    items.forEach { buffer.putInt(it) }

    return buffer.array()
}

fun toByteArray(vararg items: Long): ByteArray {
    val buffer = ByteBuffer.allocate(Long.SIZE_BYTES * items.size)
    items.forEach { buffer.putLong(it) }

    return buffer.array()
}

fun CRC32.checksum(vararg items: ByteArray): Long {
    items.forEach { update(it) }
    return value
}