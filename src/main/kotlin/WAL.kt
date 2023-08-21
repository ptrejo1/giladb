import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer

class WALCreationException: Exception()

class WALDoesNotExistException: Exception()

class WAL private constructor(private val file: File) {

    private val bufferedWriter = BufferedOutputStream(FileOutputStream(file, true))

    companion object {

        /**
         * @throws [WALCreationException]
         */
        fun create(directory: String): WAL {
            val fileName = "${System.currentTimeMillis()}.wal"
            val file = File("$directory/$fileName")
            if (!file.createNewFile())
                throw WALCreationException()

            return WAL(file)
        }

        /**
         * @throws [WALDoesNotExistException]
         */
        fun existing(path: String): WAL {
            val file = File(path)
            if (!file.isFile)
                throw WALDoesNotExistException()

            return WAL(file)
        }
    }

    fun set(entry: WALEntry) = bufferedWriter.write(entry.encode())

    fun flush() = bufferedWriter.flush()

    fun close() = bufferedWriter.close()

    fun entries() = sequence {
        val inputStream = BufferedInputStream(file.inputStream())
        val blockSizeBuffer = ByteBuffer.allocate(Int.SIZE_BYTES)
        var bytesRead = inputStream.read(blockSizeBuffer.array())

        while (bytesRead != -1) {
            val chunk = ByteArray(blockSizeBuffer.getInt(0))
            inputStream.read(chunk)
            bytesRead = inputStream.read(blockSizeBuffer.array())
            yield(WALEntry.decode(chunk))
        }

        inputStream.close()
    }
}
