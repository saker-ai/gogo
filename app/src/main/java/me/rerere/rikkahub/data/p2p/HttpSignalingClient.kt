package me.rerere.rikkahub.data.p2p

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.readRawCookies
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.http.withCharset
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.webrtc.IceCandidate
import org.webrtc.SessionDescription

/**
 * HTTP+ SSE implementation of [SignalingClient] (§4.2).
 *
 * Uses Ktor's [HttpClient] for both REST calls and the SSE events stream.
 * JWT is cached in-memory and refreshed before [AuthTokenResponse.refreshAfter].
 */
class HttpSignalingClient(
    private val client: HttpClient,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private var hubBaseUrl: String,
    private var rootToken: String,
    private var clientId: String,
) : SignalingClient {

    @Volatile private var cachedToken: AuthTokenResponse? = null

    private suspend fun ensureValidJwt(): String {
        val cached = cachedToken
        if (cached != null) {
            // Refresh 10 minutes before expiry (§7.2.2).
            val refreshAt = parseIso8601(cached.refreshAfter)
            if (refreshAt == null || refreshAt.time > System.currentTimeMillis() - 60_000) {
                return cached.jwt
            }
        }
        cachedToken = exchangeToken(rootToken, clientId)
        return cachedToken!!.jwt
    }

    override suspend fun exchangeToken(rootToken: String, clientId: String): AuthTokenResponse {
        val body = buildJsonObject {
            put("root_token", rootToken)
            put("client_id", clientId)
        }
        val resp: HttpResponse = client.post("$hubBaseUrl/api/v1/auth/token") {
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(JsonObject.serializer(), body))
        }
        if (resp.status.value != 200) {
            error("auth/token failed: ${resp.status}")
        }
        return json.decodeFromString(AuthTokenResponse.serializer(), resp.body())
    }

    override suspend fun refreshJwt(): AuthTokenResponse {
        cachedToken = exchangeToken(rootToken, clientId)
        return cachedToken!!
    }

    override suspend fun sendOffer(
        targetPeerId: String,
        clientId: String,
        sdp: SessionDescription,
    ): SignalResponse {
        val jwt = ensureValidJwt()
        val body = buildJsonObject {
            put("target_peer", targetPeerId)
            put("client_id", clientId)
            put("sdp", buildJsonObject {
                put("type", sdp.type.canonicalForm().lowercase())
                put("sdp", sdp.description)
            })
        }
        val resp: HttpResponse = client.post("$hubBaseUrl/api/v1/signal/offer") {
            header(HttpHeaders.Authorization, "Bearer $jwt")
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(JsonObject.serializer(), body))
        }
        if (resp.status.value != 200) {
            error("signal/offer failed: ${resp.status}")
        }
        return json.decodeFromString(SignalResponse.serializer(), resp.body())
    }

    override suspend fun sendIceCandidate(sessionId: String, candidate: IceCandidate) {
        val jwt = ensureValidJwt()
        val body = buildJsonObject {
            put("session_id", sessionId)
            put("candidate", buildJsonObject {
                put("candidate", candidate.sdp)
                put("sdpMid", candidate.sdpMid ?: "0")
                put("sdpMLineIndex", candidate.sdpMLineIndex)
            })
        }
        val resp: HttpResponse = client.post("$hubBaseUrl/api/v1/signal/ice") {
            header(HttpHeaders.Authorization, "Bearer $jwt")
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(JsonObject.serializer(), body))
        }
        if (resp.status.value != 204) {
            error("signal/ice failed: ${resp.status}")
        }
    }

    override fun events(sessionId: String): Flow<SignalEvent> = flow {
        val jwt = ensureValidJwt()
        val url = "$hubBaseUrl/api/v1/signal/events?session_id=$sessionId"
        // SSE is a long-lived text/event-stream; we read line-by-line.
        // Ktor's HttpClient supports this via the raw socket; here we use a
        // simple line-reader over the response body.
        kotlinx.coroutines.coroutineScope {
            val resp = client.get(url) {
                header(HttpHeaders.Authorization, "Bearer $jwt")
                header(HttpHeaders.Accept, "text/event-stream")
            }
            val body: ByteReadChannel = resp.body()
            val reader = io.ktor.utils.io.ByteReadChannel(body)
            val lines = io.ktor.utils.io.readLines(reader)
            for (line in lines) {
                val ev = parseSseLine(line) ?: continue
                emit(ev)
            }
        }
    }

    override suspend fun listPeers(): List<PeerInfo> {
        val jwt = ensureValidJwt()
        val resp: HttpResponse = client.get("$hubBaseUrl/api/v1/signal/peers") {
            header(HttpHeaders.Authorization, "Bearer $jwt")
        }
        if (resp.status.value != 200) {
            error("signal/peers failed: ${resp.status}")
        }
        val element = json.decodeFromString(JsonElement.serializer(), resp.body())
        val arr = (element as? JsonObject)?.get("peers") ?: return emptyList()
        return json.decodeFromString(kotlinx.serialization.builtins.ListSerializer(PeerInfo.serializer()), arr.toString())
    }

    private fun parseSseLine(line: String): SignalEvent? {
        if (line.startsWith("event: ice")) return null // data follows on next line
        if (line.startsWith("data: ")) {
            val data = line.removePrefix("data: ").trim()
            return SignalEvent.Ice(json.parseToJsonElement(data))
        }
        return null
    }

    private fun parseIso8601(s: String): java.util.Date? = try {
        javax.xml.bind.DatatypeConverter.parseDateTime(s).time
    } catch (e: Exception) {
        null
    }
}
