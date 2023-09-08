import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Items must be less than 4 KB
 */
class GilaDB private constructor(directory: Path) {

    companion object {

        fun open(path: String): GilaDB {
            val dbPath = Paths.get(path)
            resetDirectory(dbPath)

            return GilaDB(dbPath)
        }

        private fun resetDirectory(path: Path) {
            Files.deleteIfExists(path)
            Files.createDirectories(path)
        }
    }

    private val lsmPath = Paths.get(directory.toString(), "lsm")
    private val lsmTree: LSMTree

    init {
        resetDirectory(lsmPath)
        lsmTree = LSMTree.create(lsmPath.toString())
    }

    fun get(key: String): MemTableEntry? =
        lsmTree.get(key.encodeToByteArray())

    fun set(key: String, value: String) =
        lsmTree.set(key.encodeToByteArray(), value.encodeToByteArray(), false)

    fun delete(key: String) =
        lsmTree.set(key.encodeToByteArray(), byteArrayOf(), true)
}
