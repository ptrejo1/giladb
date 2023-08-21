import kotlin.math.min

val keyRange = 20 to 60
val valueRange = 100 to 1000

fun randomString(minLength: Int, maxLength: Int): String {
    val chars = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    val length = (minLength..maxLength).random()

    return (1..length).map { chars.random() }.joinToString("")
}
