abstract class Level {

    protected val sstFiles: MutableList<SST> = mutableListOf()

    abstract fun get(key: ByteArray): MemTableEntry?
}

class Level0: Level() {

    fun addSST(sst: SST) = sstFiles.add(sst)

    override fun get(key: ByteArray): MemTableEntry? {
        for (sst in sstFiles) {
            sst.get(key)?.let { return it }
        }

        return null
    }
}
