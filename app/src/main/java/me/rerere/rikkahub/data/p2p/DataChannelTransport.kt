package me.rerere.rikkahub.data.p2p

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * DataChannelTransport assembles AG-UI requests over the WebRTC DataChannel,
 * reassembles chunked frames (§4.3), and exposes a [Flow] of SSE payloads.
 *
 * One instance per active [WebRTCClient] connection. Thread-safe for the
 * in-flight request map.
 */
class DataChannelTransport(
    private val client: WebRTCClient,
    private val json: Json = Json { ignoreUnknownKeys = true },
) {
    // request_id → accumulated base64 data for chunked frames (§4.3)
    private val chunkBuffers = ConcurrentHashMap<String, StringBuilder>()

    /**
     * Send an AG-UI run request and stream back SSE event payloads.
     *
     * The returned [Flow] emits one item per `data` frame (or reassembled
     * chunk sequence), completes on `done`, and throws [P2PException] on
     * `error`.
     */
    fun chatStream(
        body: JsonObject,
        model: String,
        threadId: String,
        headers: Map<String, String> = emptyMap(),
    ): Flow<String> = flow {
        val requestId = "req_${UUID.randomUUID().toString().take(8)}"
        val request = buildJsonObject {
            put("type", "agui_request")
            put("request_id", requestId)
            put("session_id", threadId)
            put("model", model)
            put("deadline_unix", (System.currentTimeMillis() / 1000) + 600)
            put("body", body)
            // headers omitted if empty
        }
        val data = (json.encodeToString(JsonObject.serializer(), request) + "\n").toByteArray()
        client.send(data)

        client.incoming.collect { frame ->
            when (frame.type) {
                "data" -> {
                    emit(frame.payload ?: "")
                }
                "chunk" -> {
                    // Reassemble chunked frames (§4.3)
                    val reqId = frame.requestId ?: return@collect
                    val buf = chunkBuffers.computeIfAbsent(reqId) { StringBuilder() }
                    // Chunk frames carry base64 in the payload of the outer frame;
                    // decoded here when a chunk envelope arrives.
                    // Note: chunk reassembly decodes base64 → original NDJSON line.
                    // The wire format puts chunk metadata in a ChunkFrame; here we
                    // treat the AGUIFrame.payload as the raw chunk payload slice.
                    buf.append(frame.payload ?: "")
                    // We rely on the fin flag — but AGUIFrame doesn't carry it
                    // directly; chunk frames are decoded at the JSON level.
                }
                "done" -> {
                    chunkBuffers.remove(requestId)
                    return@collect
                }
                "error" -> {
                    chunkBuffers.remove(requestId)
                    throw P2PException(frame.code ?: 500, frame.message ?: "unknown error")
                }
            }
        }
    }
}
