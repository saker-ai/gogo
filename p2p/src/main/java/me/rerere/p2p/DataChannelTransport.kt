package me.rerere.p2p

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
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
 *
 * ## Wire format contract (peer-side)
 *
 * Saker nodes MUST send large payloads (base64 images, embedding vectors,
 * long tool outputs) as `chunk` frames per §4.3, NOT as a single oversized
 * `data` frame. WebRTC DataChannel has a ~16 KiB per-message ceiling on the
 * default SCTP config; payloads larger than that would be silently truncated
 * if sent as a single `data` frame.
 *
 * Frame format the peer must emit:
 * - `data` frame: `{"type":"data","request_id":"...","payload":"<utf8 sse>"}` — small text deltas, tool call args, run_finished, etc.
 * - `chunk` frame: `{"type":"chunk","request_id":"...","payload":"{\"seq\":0,\"fin\":false,\"data\":\"<base64-piece>\"}"}` — multi-frame, base64-encoded, last frame has `fin:true`. The transport base64-decodes and concatenates `data` across all chunks for the same `request_id` before emitting the reassembled UTF-8 payload.
 * - `done` frame: `{"type":"done","request_id":"..."}` — stream terminator.
 * - `error` frame: `{"type":"error","request_id":"...","code":500,"message":"..."}` — stream-level error; aborts the flow with [P2PException].
 *
 * Callers above this layer (e.g. [SakerP2PSseCodec]) only ever see the final
 * UTF-8 SSE payload — chunked reassembly is transparent.
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

    // Thrown by the state monitor when the connection drops mid-stream so the
    // incoming collect aborts immediately instead of hanging until the caller's
    // withTimeoutOrNull fires.
    private class StreamAborted(val state: P2PConnectionState) : RuntimeException() {
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
            // Wrap in coroutineScope so a state-monitor child coroutine can
            // throw StreamAborted to cancel the incoming.collect sibling on
            // connection drop. Without this, collect would hang on a dead
            // SharedFlow (MutableSharedFlow never completes) until the
            // caller's withTimeoutOrNull fires.
            coroutineScope {
                val stateMonitor = launch {
                    client.state.collect { state ->
                        if (state is P2PConnectionState.Disconnected ||
                            state is P2PConnectionState.Failed
                        ) {
                            throw StreamAborted(state)
                        }
                    }
                }
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
                                val decoded = reassembleChunk(chunkBuffers, reqId, chunk)
                                if (decoded != null) emit(decoded)
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
                } finally {
                    stateMonitor.cancel()
                }
            }
        } catch (e: StreamDone) {
            // Normal stream termination; flow completes.
        } catch (e: StreamAborted) {
            chunkBuffers.remove(requestId)
            throw P2PException(503, "connection ${e.state} mid-stream")
        }
    }
}

/**
 * Accumulate a [ChunkFrame] into the per-request buffer and, if `fin` is set,
 * base64-decode the concatenated payload to a UTF-8 string.
 *
 * Returns null while accumulating (more chunks expected), or the decoded
 * payload on the final chunk. Returns null on the final chunk if base64
 * decoding fails — the caller silently drops the malformed sequence.
 *
 * Extracted as a top-level internal function so the reassembly logic can
 * be unit-tested without a live WebRTC transport.
 */
internal fun reassembleChunk(
    buffers: ConcurrentHashMap<String, StringBuilder>,
    requestId: String,
    chunk: ChunkFrame,
): String? {
    val buf = buffers.computeIfAbsent(requestId) { StringBuilder() }
    buf.append(chunk.data)
    if (!chunk.fin) return null
    buffers.remove(requestId)
    return try {
        Base64.getDecoder().decode(buf.toString()).toString(Charsets.UTF_8)
    } catch (e: Exception) {
        null
    }
}
