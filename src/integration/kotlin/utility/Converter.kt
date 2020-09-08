package utility

import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.Parser
import com.beust.klaxon.lookup
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.CRC32

open class Converter {

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy { it })
        val json = JsonObject(sortedEntries)
        return json.toJsonString()
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.value.toInt()).array()
    }

    fun encodeToBase64(input: String): String {
        return Base64.getEncoder().encodeToString(input.toByteArray())
    }

    fun decodeFromBase64(input: String): String {
        val decodedBytes: ByteArray = Base64.getDecoder().decode(input)
        return String(decodedBytes)
    }

    open fun getTimestampAsLong(timeStampAsStr: String?, timeStampPattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"): Long {
        val df = SimpleDateFormat(timeStampPattern)
        return df.parse(timeStampAsStr).time
    }

    open fun getLastModifiedTimestamp(json: JsonObject?): Pair<String, String> {
        val epoch = "1980-01-01T00:00:00.000+0000"
        val lastModifiedTimestampStr = json?.lookup<String?>("message._lastModifiedDateTime")?.get(0)
        if (!lastModifiedTimestampStr.isNullOrBlank()) {
            return Pair(lastModifiedTimestampStr, "_lastModifiedDateTime")
        }
        
        val createdTimestampStr = json?.lookup<String?>("message.createdDateTime")?.get(0)
        if (!createdTimestampStr.isNullOrBlank()) {
            return Pair(createdTimestampStr, "createdDateTime")
        }

        return Pair(epoch, "epoch")
    }

}
