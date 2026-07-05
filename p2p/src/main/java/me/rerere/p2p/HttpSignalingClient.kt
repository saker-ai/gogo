package me.rerere.p2p

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import me.rerere.common.http.await
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import okhttp3.sse.EventSources
import org.webrtc.IceCandidate
import org.webrtc.SessionDescription
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * HTTP + SSE implementation of [SignalingClient] (§4.2 of the design doc).
 *
 * Uses OkHttp for both REST calls and the SSE events stream, consistent with
 * the rest of the gogo codebase. JWT is cached in-memory and refreshed before
 * [AuthTokenResponse.refreshAfter].
 */
class HttpSignalingClient(
    private val client: OkHttpClient,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private var hubBaseUrl: String,
    private var rootToken: String,
    private var clientId: String,
) : SignalingClient {

    @Volatile private var cachedToken: AuthTokenResponse? = null

    private fun jsonBody(content: String) = content.toRequestBody("application/json".toMediaType())

    private suspend fun ensureValidJwt(): String {
        val cached = cachedToken
        if (cached != null) {
            // Refresh 10 minutes before expiry (§7.2.2).
            val refreshAt = parseIso8601(cached.refreshAfter)
            val now = System.currentTimeMillis()
            if (refreshAt == null || refreshAt > now - 60_000) {
                return cached.jwt
            }
        }
        cachedToken = exchangeToken(rootToken, clientId)
        return cachedToken!!.jwt
    }

    override suspend fun exchangeToken(rootToken: String, clientId: String): AuthTokenResponse =
        withContext(Dispatchers.IO) {
            val body = buildJsonObject {
                put("root_token", rootToken)
                put("client_id", clientId)
            }
            val req = Request.Builder()
                .url("$hubBaseUrl/api/v1/auth/token")
                .post(jsonBody(json.encodeToString(JsonObject.serializer(), body)))
                .build()
            val resp = client.newCall(req).await()
            if (!resp.isSuccessful) {
                error("auth/token failed: ${resp.code}")
            }
            json.decodeFromString(AuthTokenResponse.serializer(), resp.body!!.string())
        }

    override suspend fun refreshJwt(): AuthTokenResponse {
        cachedToken = exchangeToken(rootToken, clientId)
        return cachedToken!!
    }

    override suspend fun sendOffer(
        targetPeerId: String,
        clientId: String,
        sdp: SessionDescription,
    ): SignalResponse = withContext(Dispatchers.IO) {
        val jwt = ensureValidJwt()
        val body = buildJsonObject {
            put("target_peer", targetPeerId)
            put("client_id", clientId)
            put("sdp", buildJsonObject {
                put("type", sdp.type.name.lowercase())
                put("sdp", sdp.description)
            })
        }
        val req = Request.Builder()
            .url("$hubBaseUrl/api/v1/signal/offer")
            .header("Authorization", "Bearer $jwt")
            .post(jsonBody(json.encodeToString(JsonObject.serializer(), body)))
            .build()
        val resp = client.newCall(req).await()
        if (!resp.isSuccessful) {
            error("signal/offer failed: ${resp.code}")
        }
        json.decodeFromString(SignalResponse.serializer(), resp.body!!.string())
    }

    override suspend fun sendIceCandidate(sessionId: String, candidate: IceCandidate) =
        withContext(Dispatchers.IO) {
            val jwt = ensureValidJwt()
            val body = buildJsonObject {
                put("session_id", sessionId)
                put("candidate", buildJsonObject {
                    put("candidate", candidate.sdp)
                    put("sdpMid", candidate.sdpMid ?: "0")
                    put("sdpMLineIndex", candidate.sdpMLineIndex)
                })
            }
            val req = Request.Builder()
                .url("$hubBaseUrl/api/v1/signal/ice")
                .header("Authorization", "Bearer $jwt")
                .post(jsonBody(json.encodeToString(JsonObject.serializer(), body)))
                .build()
            val resp = client.newCall(req).await()
            if (resp.code != 204) {
                error("signal/ice failed: ${resp.code}")
            }
        }

    override fun events(sessionId: String): Flow<SignalEvent> = callbackFlow {
        val jwt = ensureValidJwt()
        val url = "$hubBaseUrl/api/v1/signal/events?session_id=$sessionId"
        val req = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $jwt")
            .header("Accept", "text/event-stream")
            .build()

        val factory = EventSources.createFactory(client)
        val source = factory.newEventSource(req, object : EventSourceListener() {
            private var currentEvent: String? = null

            override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
                when (type) {
                    "ice" -> {
                        val element = try {
                            json.parseToJsonElement(data)
                        } catch (e: Exception) {
                            return
                        }
                        trySend(SignalEvent.Ice(element))
                    }
                    "connected" -> trySend(SignalEvent.Connected(sessionId))
                    "failed" -> trySend(SignalEvent.Failed(sessionId, data))
                }
            }

            override fun onClosed(eventSource: EventSource) {
                channel.close()
            }

            override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
                channel.close(t)
            }
        })
        awaitClose { source.cancel() }
    }

    override suspend fun listPeers(): List<PeerInfo> = withContext(Dispatchers.IO) {
        val jwt = ensureValidJwt()
        val req = Request.Builder()
            .url("$hubBaseUrl/api/v1/signal/peers")
            .header("Authorization", "Bearer $jwt")
            .get()
            .build()
        val resp = client.newCall(req).await()
        if (!resp.isSuccessful) {
            error("signal/peers failed: ${resp.code}")
        }
        parsePeersResponseBody(json, resp.body!!.string())
    }

    private fun parseIso8601(s: String): Long? = try {
        // Accept both Instant format (with 'Z') and offset formats.
        Instant.parse(s).toEpochMilli()
    } catch (e: DateTimeParseException) {
        try {
            java.time.OffsetDateTime.parse(s).toInstant().toEpochMilli()
        } catch (e2: DateTimeParseException) {
            null
        }
    }
}

/**
 * Parse the body of GET /api/v1/signal/peers into a [PeerInfo] list.
 * Extracted as a top-level internal function so it can be unit-tested
 * without an OkHttpClient.
 *
 * Returns an empty list if the body is missing the `peers` field or the
 * field isn't an array. Malformed JSON propagates as a serialization
 * exception — the caller's [listPeers] wraps the request in a try/catch
 * boundary.
 */
internal fun parsePeersResponseBody(
    json: Json,
    body: String,
): List<PeerInfo> {
    val element = json.parseToJsonElement(body)
    val arr = (element as? JsonObject)?.get("peers") as? JsonArray ?: return emptyList()
    return json.decodeFromString(
        kotlinx.serialization.builtins.ListSerializer(PeerInfo.serializer()),
        arr.toString(),
    )
}
