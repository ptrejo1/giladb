class LSMTree private constructor(private val directory: String, private val levels: Int = 3) {

    private val memTable = MemTable()

    companion object {

        fun create(directory: String): LSMTree {
            return LSMTree(directory)
        }
    }

    fun get() {}

    fun set() {}

    fun delete() {}
}
