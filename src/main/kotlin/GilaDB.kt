import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Items must be less than 4 KB
 */
class GilaDB private constructor(private val directory: Path) {

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
    private val walPath = Paths.get(directory.toString(), "wal")
    private val wal: WAL

    init {
        resetDirectory(walPath)
        wal = WAL.create(walPath.toString())

        resetDirectory(lsmPath)
        lsmTree = LSMTree.create(lsmPath.toString())
    }

    fun get() {}

    fun set() {}

    fun delete() {}
}
