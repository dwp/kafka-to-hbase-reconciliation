package utility

import com.beust.klaxon.JsonObject

open class MessageParser {

    private val converter = Converter()

    open fun generateKeyFromRecordBody(body: JsonObject?): ByteArray {
        val id: JsonObject? = body?.let { getId(it) }
        return if (id == null) ByteArray(0) else generateKey(id)
    }

    private fun getId(json: JsonObject): JsonObject? {
        val message = json["message"]
        if (message != null && message is JsonObject) {
            val id = message["_id"]

            if (id != null) {
                when (id) {
                    is JsonObject -> {
                        return id
                    }
                    is String -> {
                        val idObject = JsonObject()
                        idObject["id"] = id
                        return idObject
                    }
                    is Int -> {
                        val idObject = JsonObject()
                        idObject["id"] = "$id"
                        return idObject
                    }
                    else -> {
                        return null
                    }
                }
            }
            else {
                return null
            }

        }
        else {
            return null
        }
    }

    private fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = converter.sortJsonByKey(json)
        val checksumBytes: ByteArray = converter.generateFourByteChecksum(jsonOrdered)
        return checksumBytes.plus(jsonOrdered.toByteArray())
    }

    fun printableKey(key: ByteArray): String {
        val hash = key.slice(IntRange(0, 3))
        val hex = hash.joinToString("") { String.format("\\x%02X", it) }
        val renderable = key.slice(IntRange(4, key.size - 1)).map{ it.toChar() }.joinToString("")
        return "${hex}${renderable}"
    }

}
