package me.rerere.rikkahub.data.p2p

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.Base64
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

    // Used to break out of collect on normal stream termination. crossinline
    // lambdas cannot use non-local return, but they can throw.
    private class StreamDone : RuntimeException() {
        override fun fillInStackTrace(): Throwable = this
    }

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

        try {
            client.incoming.collect { frame ->
                // Route by request_id; skip auth/connection-level frames.
                val frameReqId = frame.requestId
                if (frameReqId != null && frameReqId != requestId) return@collect

                when (frame.type) {
                    "data" -> {
                        emit(frame.payload ?: "")
                    }
                    "chunk" -> {
                        val reqId = frame.requestId ?: return@collect
                        val payloadStr = frame.payload ?: return@collect
                        val chunk = try {
                            json.decodeFromString(ChunkFrame.serializer(), payloadStr)
                        } catch (e: Exception) {
                            return@collect
                        }
                        val buf = chunkBuffers.computeIfAbsent(reqId) { StringBuilder() }
                        buf.append(chunk.data)
                        if (chunk.fin) {
                            chunkBuffers.remove(reqId)
                            val decoded = try {
                                Base64.getDecoder().decode(buf.toString()).toString(Charsets.UTF_8)
                            } catch (e: Exception) {
                                return@collect
                            }
                            emit(decoded)
                        }
                    }
                    "done" -> {
                        chunkBuffers.remove(requestId)
                        throw StreamDone()
                    }
                    "error" -> {
                        chunkBuffers.remove(requestId)
                        throw P2PException(frame.code ?: 500, frame.message ?: "unknown error")
                    }
                }
            }
        } catch (e: StreamDone) {
            // Normal stream termination; flow completes.
        }
    }
}
