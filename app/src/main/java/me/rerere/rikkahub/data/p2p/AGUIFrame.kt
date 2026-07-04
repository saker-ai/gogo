package me.rerere.rikkahub.data.p2p

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/**
 * AGUIFrame is the application-layer envelope exchanged over the DataChannel
 * (§4.3 of the design doc). One frame per NDJSON line.
 */
@Serializable
data class AGUIFrame(
    val type: String,
    @SerialName("request_id")
    val requestId: String? = null,
    val payload: String? = null,
    val code: Int? = null,
    val message: String? = null,
)

/**
 * ChunkFrame is the transport-layer fragmentation envelope (§4.3). chunk.data
 * is always base64-encoded.
 */
@Serializable
data class ChunkFrame(
    val type: String,
    @SerialName("request_id")
    val requestId: String,
    val seq: Int,
    val fin: Boolean,
    val data: String,
)

/**
 * P2P exception thrown by the Saker node via the DataChannel error frame.
 */
class P2PException(val code: Int, message: String) : RuntimeException(message)
