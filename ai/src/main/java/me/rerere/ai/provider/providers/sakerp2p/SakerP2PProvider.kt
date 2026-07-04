package me.rerere.ai.provider.providers.sakerp2p

import android.content.Context
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import me.rerere.ai.provider.EmbeddingGenerationParams
import me.rerere.ai.provider.EmbeddingGenerationResult
import me.rerere.ai.provider.ImageEditParams
import me.rerere.ai.provider.ImageGenerationParams
import me.rerere.ai.provider.ImageGenerationItem
import me.rerere.ai.provider.Model
import me.rerere.ai.provider.Provider
import me.rerere.ai.provider.ProviderSetting
import me.rerere.ai.provider.TextGenerationParams
import me.rerere.ai.ui.ImageAspectRatio
import me.rerere.ai.ui.MessageChunk
import me.rerere.ai.ui.UIMessage
import me.rerere.rikkahub.data.p2p.DataChannelTransport
import me.rerere.rikkahub.data.p2p.HttpSignalingClient
import me.rerere.rikkahub.data.p2p.WebRTCClient
import okhttp3.OkHttpClient
import java.util.UUID

/**
 * SakerP2PProvider bridges gogo's Provider abstraction to a Saker node reachable
 * over WebRTC P2P (§3.3.4 of the design doc).
 *
 * The WebRTC connection lifecycle is owned by [WebRTCClient]; this class only
 * builds AG-UI run requests and streams back SSE events parsed into
 * [MessageChunk]s. Connection establishment is lazy: the first call to
 * [streamText] triggers connect() if no connection is established.
 */
class SakerP2PProvider(
    private val client: OkHttpClient,
    private val context: Context,
) : Provider<ProviderSetting.SakerP2P> {

    private val json = Json { ignoreUnknownKeys = true }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile private var webrtcClient: WebRTCClient? = null
    @Volatile private var transport: DataChannelTransport? = null
    @Volatile private var signalingClient: HttpSignalingClient? = null
    @Volatile private var connectedSetting: ProviderSetting.SakerP2P? = null

    override suspend fun listModels(providerSetting: ProviderSetting.SakerP2P): List<Model> {
        // Saker nodes advertise their own model list via the Hub peers API;
        // for now we return whatever the user has configured on the setting.
        return providerSetting.models
    }

    override suspend fun generateText(
        providerSetting: ProviderSetting.SakerP2P,
        messages: List<UIMessage>,
        params: TextGenerationParams,
    ): MessageChunk {
        // Saker P2P is stream-only; collect the stream into a single chunk.
        val chunks = streamText(providerSetting, messages, params)
        val sb = StringBuilder()
        chunks.collect { chunk ->
            chunk.delta?.text?.let { sb.append(it) }
        }
        return MessageChunk(delta = me.rerere.ai.ui.MessageDelta(text = sb.toString()))
    }

    override suspend fun streamText(
        providerSetting: ProviderSetting.SakerP2P,
        messages: List<UIMessage>,
        params: TextGenerationParams,
    ): Flow<MessageChunk> = flow {
        ensureConnected(providerSetting)
        val transport = transport ?: error("P2P transport not initialized")

        val threadId = params.sessionId ?: "mobile-thread-${UUID.randomUUID().toString().take(8)}"
        val body = buildAGUIRunRequest(messages, params)

        transport.chatStream(
            body = body,
            model = params.model,
            threadId = threadId,
        ).collect { ssePayload ->
            // Parse the SSE event payload (an AG-UI event line) into a MessageChunk.
            val chunk = parseSSEPayload(ssePayload)
            if (chunk != null) emit(chunk)
        }
    }

    override suspend fun generateImage(
        providerSetting: ProviderSetting.SakerP2P,
        params: ImageGenerationParams,
    ): Flow<ImageGenerationItem> {
        error("Image generation is not supported over Saker P2P")
    }

    private fun buildAGUIRunRequest(
        messages: List<UIMessage>,
        params: TextGenerationParams,
    ): JsonObject = buildJsonObject {
        put("threadId", params.sessionId ?: "mobile-thread")
        putJsonArray("messages") {
            for (m in messages) {
                add(buildJsonObject {
                    put("role", m.role.name.lowercase())
                    put("content", m.contentText())
                })
            }
        }
        putJsonArray("tools") { /* tools serialization deferred to AG-UI builder */ }
    }

    private fun parseSSEPayload(payload: String): MessageChunk? {
        // AG-UI SSE payloads look like "event: message\ndata: {...}\n\n"
        // The DataChannelTransport already strips outer framing; here we parse
        // the inner data JSON and extract a text delta.
        val dataLine = payload.lines().firstOrNull { it.startsWith("data: ") } ?: return null
        val data = dataLine.removePrefix("data: ").trim()
        if (data.isEmpty()) return null
        return try {
            val element = json.parseToJsonElement(data)
            val obj = element as? JsonObject ?: return null
            val delta = obj["delta"] as? JsonObject ?: return null
            val text = (delta["text"]?.let { it as? kotlinx.serialization.json.JsonPrimitive })?.content ?: ""
            MessageChunk(delta = me.rerere.ai.ui.MessageDelta(text = text))
        } catch (e: Exception) {
            null
        }
    }

    private suspend fun ensureConnected(setting: ProviderSetting.SakerP2P) {
        if (webrtcClient != null && connectedSetting?.id == setting.id) return

        // Resolve clientId once so signaling and WebRTC see the same value.
        val clientId = setting.clientId.ifEmpty { "gogo-${UUID.randomUUID().toString().take(8)}" }

        val signaling = HttpSignalingClient(
            client = this.client,
            hubBaseUrl = setting.hubUrl,
            rootToken = setting.authToken,
            clientId = clientId,
        )
        signalingClient = signaling

        val webrtc = WebRTCClient(context, signaling, scope)
        webrtcClient = webrtc
        transport = DataChannelTransport(webrtc)

        // ICE servers default to Google STUN; the Hub will return its own list
        // as part of the offer response, but we need a baseline for the createOffer call.
        val iceServers = listOf(
            org.webrtc.PeerConnection.IceServer.builder("stun:stun.l.google.com:19302").createIceServer(),
        )

        // Exchange root token for JWT before connecting.
        val token = signaling.refreshJwt()
        webrtc.connect(setting.targetPeerId, clientId, iceServers, token.jwt)
        connectedSetting = setting
    }

    fun disconnect() {
        webrtcClient?.disconnect()
        webrtcClient = null
        transport = null
        signalingClient = null
        connectedSetting = null
    }
}

// Helper extension to extract plain text content from a UIMessage.
private fun UIMessage.contentText(): String {
    return parts.joinToString("") { part ->
        when (part) {
            is me.rerere.ai.ui.UIMessagePart.Text -> part.text
            else -> ""
        }
    }
}
