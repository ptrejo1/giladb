import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.reflect.full.companionObject
import java.nio.ByteBuffer
import java.util.*
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

class GilaByteArray {

    companion object {

        /**
         * @see Arrays.compare
         */
        fun compare(a: ByteArray, b: ByteArray): Int =
            Arrays.compare(a, b)
    }
}

fun <T: Any> T.getLogger(): Lazy<Logger> {
    return lazy { LoggerFactory.getLogger(unwrapCompanionClass(this.javaClass).name) }
}

fun <T: Any> unwrapCompanionClass(ofClass: Class<T>): Class<*> {
    return ofClass.enclosingClass?.takeIf {
        ofClass.enclosingClass.kotlin.companionObject?.java == ofClass
    } ?: ofClass
}
