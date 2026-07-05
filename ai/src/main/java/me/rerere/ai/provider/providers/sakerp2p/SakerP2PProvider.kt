package me.rerere.ai.provider.providers.sakerp2p

import android.content.Context
import android.util.Log
import java.io.Closeable
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.uuid.Uuid
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import me.rerere.ai.core.MessageRole
import me.rerere.ai.provider.EmbeddingGenerationParams
import me.rerere.ai.provider.EmbeddingGenerationResult
import me.rerere.ai.provider.ImageEditParams
import me.rerere.ai.provider.ImageGenerationParams
import me.rerere.ai.provider.Model
import me.rerere.ai.provider.Provider
import me.rerere.ai.provider.ProviderSetting
import me.rerere.ai.provider.TextGenerationParams
import me.rerere.ai.registry.ModelRegistry
import me.rerere.ai.ui.ImageGenerationItem
import me.rerere.ai.ui.MessageChunk
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessageChoice
import me.rerere.ai.ui.UIMessagePart
import me.rerere.p2p.DataChannelTransport
import me.rerere.p2p.HttpSignalingClient
import me.rerere.p2p.P2PConnectionState
import me.rerere.p2p.WebRTCClient
import okhttp3.OkHttpClient
import org.webrtc.PeerConnection

/**
 * SakerP2PProvider bridges gogo's Provider abstraction to a Saker node reachable
 * over WebRTC P2P (§3.3.4 of the design doc).
 *
 * The WebRTC connection lifecycle is owned by [WebRTCClient]; this class builds
 * AG-UI run requests and streams back SSE events parsed into [MessageChunk]s.
 * Connection establishment is lazy: the first call to [streamText] triggers
 * [ensureConnected] if no connection is established.
 *
 * Connection state is exposed via [connectionState] so the UI can show
 * Connecting/Connected/Reconnecting/Failed status.
 *
 * Threading model: all [chatStream] calls are serialized through
 * [transportMutex] because [WebRTCClient.incoming] is a SharedFlow that
 * broadcasts every frame to every collector — concurrent calls would
 * both decode all frames and double the work.
 */
class SakerP2PProvider(
    private val client: OkHttpClient,
    private val context: Context,
) : Provider<ProviderSetting.SakerP2P>, Closeable {

    private val json = Json { ignoreUnknownKeys = true }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val transportMutex = Mutex()

    @Volatile private var webrtcClient: WebRTCClient? = null
    @Volatile private var transport: DataChannelTransport? = null
    @Volatile private var signalingClient: HttpSignalingClient? = null
    @Volatile private var connectedSetting: ProviderSetting.SakerP2P? = null
    @Volatile private var connectionJob: Job? = null

    // Per-provider model cache (keyed by setting.id) so switching between
    // multiple SakerP2P providers never returns another peer's model list.
    private data class ModelsCacheEntry(val models: List<Model>, val timestamp: Long)
    private val modelsCache = ConcurrentHashMap<String, ModelsCacheEntry>()

    // Reconnect cooldown: after a Failed state, refuse immediate reconnect so
    // a down peer doesn't trigger a request-fail-reconnect tight loop.
    @Volatile private var lastFailedAt: Long = 0L

    // ICE servers: start with Google STUN as a baseline. Upper layers can push
    // a Hub-provided or remote-config list via [updateIceServers]; the Hub
    // also returns iceServers in SignalResponse, but WebRTCClient needs the
    // list before createOffer — so we serve from cache here.
    @Volatile
    private var cachedIceServers: List<PeerConnection.IceServer> = listOf(
        PeerConnection.IceServer.builder("stun:stun.l.google.com:19302").createIceServer(),
    )

    fun updateIceServers(servers: List<PeerConnection.IceServer>) {
        if (servers.isNotEmpty()) {
            cachedIceServers = servers
            Log.i(TAG, "ICE servers updated: ${servers.size} entries")
        }
    }

    private val _connectionState = MutableStateFlow<P2PConnectionState?>(null)
    val connectionState: StateFlow<P2PConnectionState?> = _connectionState.asStateFlow()

    companion object {
        private const val TAG = "SakerP2PProvider"
        private const val STREAM_TIMEOUT_MS = 120_000L
        private const val MODELS_CACHE_TTL_MS = 60_000L
        private const val RECONNECT_COOLDOWN_MS = 3_000L
    }

    override suspend fun listModels(providerSetting: ProviderSetting.SakerP2P): List<Model> {
        val cacheKey = providerSetting.id.toString()
        val now = System.currentTimeMillis()
        val cached = modelsCache[cacheKey]
        if (cached != null && now - cached.timestamp < MODELS_CACHE_TTL_MS) {
            return cached.models
        }

        return try {
            ensureConnected(providerSetting)
            val signaling = signalingClient ?: return providerSetting.models
            val peers = signaling.listPeers()
            val targetPeer = peers.firstOrNull { it.peerId == providerSetting.targetPeerId }
            val peerModels = targetPeer?.models
            val resolved = if (peerModels.isNullOrEmpty()) {
                providerSetting.models
            } else {
                peerModels.map { modelId ->
                    val existing = providerSetting.models.find { it.modelId == modelId }
                    existing ?: Model(
                        modelId = modelId,
                        displayName = modelId,
                        inputModalities = ModelRegistry.MODEL_INPUT_MODALITIES.getData(modelId),
                        outputModalities = ModelRegistry.MODEL_OUTPUT_MODALITIES.getData(modelId),
                        abilities = ModelRegistry.MODEL_ABILITIES.getData(modelId),
                    )
                }
            }
            modelsCache[cacheKey] = ModelsCacheEntry(resolved, now)
            resolved
        } catch (e: Exception) {
            Log.w(TAG, "listModels failed, falling back to user models", e)
            providerSetting.models
        }
    }

    // P2P channels don't carry a balance concept; return a stable placeholder
    // so the UI doesn't render the inherited "TODO" default from Provider.
    override suspend fun getBalance(providerSetting: ProviderSetting.SakerP2P): String = "N/A"

    override suspend fun generateText(
        providerSetting: ProviderSetting.SakerP2P,
        messages: List<UIMessage>,
        params: TextGenerationParams,
    ): MessageChunk {
        // Collect the stream preserving all part types (text + reasoning +
        // tool calls) so non-streaming callers see the full response, not
        // just text.
        val parts = mutableListOf<UIMessagePart>()
        var finishReason: String? = null
        streamText(providerSetting, messages, params).collect { chunk ->
            val choice = chunk.choices.firstOrNull() ?: return@collect
            choice.delta?.parts?.let { parts.addAll(it) }
            choice.finishReason?.let { finishReason = it }
        }
        return MessageChunk(
            id = "p2p-${UUID.randomUUID().toString().take(8)}",
            model = params.model.modelId,
            choices = listOf(
                UIMessageChoice(
                    index = 0,
                    delta = null,
                    message = UIMessage(
                        role = MessageRole.ASSISTANT,
                        parts = parts,
                    ),
                    finishReason = finishReason ?: "stop",
                )
            ),
            usage = null,
        )
    }

    override suspend fun streamText(
        providerSetting: ProviderSetting.SakerP2P,
        messages: List<UIMessage>,
        params: TextGenerationParams,
    ): Flow<MessageChunk> = flow {
        ensureConnected(providerSetting)
        val transport = transport ?: error("P2P transport not initialized")

        val threadId = "mobile-thread-${UUID.randomUUID().toString().take(8)}"
        val body = SakerP2PSseCodec.buildAGUIRunRequest(messages, threadId, params.tools)

        withTimeoutOrNull(STREAM_TIMEOUT_MS) {
            transportMutex.withLock {
                transport.chatStream(
                    body = body,
                    model = params.model.modelId,
                    threadId = threadId,
                ).collect { ssePayload ->
                    val chunk = SakerP2PSseCodec.parseSSEPayload(ssePayload)
                    if (chunk != null) emit(chunk)
                }
            }
        }
    }

    override suspend fun generateImage(
        providerSetting: ProviderSetting,
        params: ImageGenerationParams,
    ): Flow<ImageGenerationItem> = flow {
        val sakerSetting = providerSetting as? ProviderSetting.SakerP2P
            ?: error("Saker P2P provider required for image generation")
        ensureConnected(sakerSetting)
        val transport = transport ?: error("P2P transport not initialized")

        val threadId = "img-thread-${UUID.randomUUID().toString().take(8)}"
        val body = buildJsonObject {
            put("threadId", threadId)
            put("image_generation", buildJsonObject {
                put("model", params.model.modelId)
                put("prompt", params.prompt)
                put("num_of_images", params.numOfImages)
                put("aspect_ratio", params.aspectRatio.name)
                put("partial_images", params.partialImages)
            })
        }

        withTimeoutOrNull(STREAM_TIMEOUT_MS) {
            transportMutex.withLock {
                transport.chatStream(
                    body = body,
                    model = params.model.modelId,
                    threadId = threadId,
                ).collect { ssePayload ->
                    SakerP2PSseCodec.parseImageSSEPayload(ssePayload)?.let { emit(it) }
                }
            }
        }
    }

    override suspend fun editImage(
        providerSetting: ProviderSetting,
        params: ImageEditParams,
    ): Flow<ImageGenerationItem> = flow {
        val sakerSetting = providerSetting as? ProviderSetting.SakerP2P
            ?: error("Saker P2P provider required for image editing")
        ensureConnected(sakerSetting)
        val transport = transport ?: error("P2P transport not initialized")

        val threadId = "img-edit-${UUID.randomUUID().toString().take(8)}"
        val body = buildJsonObject {
            put("threadId", threadId)
            put("image_edit", buildJsonObject {
                put("model", params.model.modelId)
                put("prompt", params.prompt)
                putJsonArray("images") {
                    params.images.forEach { add(JsonPrimitive(it)) }
                }
                put("num_of_images", params.numOfImages)
                put("aspect_ratio", params.aspectRatio.name)
                put("partial_images", params.partialImages)
            })
        }

        withTimeoutOrNull(STREAM_TIMEOUT_MS) {
            transportMutex.withLock {
                transport.chatStream(
                    body = body,
                    model = params.model.modelId,
                    threadId = threadId,
                ).collect { ssePayload ->
                    SakerP2PSseCodec.parseImageSSEPayload(ssePayload)?.let { emit(it) }
                }
            }
        }
    }

    override suspend fun generateEmbedding(
        providerSetting: ProviderSetting.SakerP2P,
        params: EmbeddingGenerationParams,
    ): EmbeddingGenerationResult {
        ensureConnected(providerSetting)
        val transport = transport ?: error("P2P transport not initialized")

        val threadId = "emb-thread-${UUID.randomUUID().toString().take(8)}"
        val body = buildJsonObject {
            put("threadId", threadId)
            put("embedding", buildJsonObject {
                put("model", params.model.modelId)
                putJsonArray("input") {
                    params.input.forEach { add(JsonPrimitive(it)) }
                }
                params.dimensions?.let { put("dimensions", it) }
            })
        }

        var result: EmbeddingGenerationResult? = null
        withTimeoutOrNull(STREAM_TIMEOUT_MS) {
            transportMutex.withLock {
                transport.chatStream(
                    body = body,
                    model = params.model.modelId,
                    threadId = threadId,
                ).collect { ssePayload ->
                    SakerP2PSseCodec.parseEmbeddingSSEPayload(ssePayload)?.let { result = it }
                }
            }
        }
        return result ?: error("Failed to generate embedding via P2P")
    }

    private suspend fun ensureConnected(setting: ProviderSetting.SakerP2P) {
        // Reconnect cooldown: if the last attempt Failed recently, wait out
        // the cooldown before re-attempting. This avoids a hot loop when the
        // peer is unreachable.
        val nowMs = System.currentTimeMillis()
        val sinceFail = nowMs - lastFailedAt
        if (lastFailedAt > 0L && sinceFail < RECONNECT_COOLDOWN_MS) {
            val wait = RECONNECT_COOLDOWN_MS - sinceFail
            Log.i(TAG, "Reconnect cooldown active, waiting ${wait}ms")
            kotlinx.coroutines.delay(wait)
        }

        val current = webrtcClient
        val currentSetting = connectedSetting

        if (current != null && currentSetting != null &&
            currentSetting.id == setting.id &&
            currentSetting.hubUrl == setting.hubUrl &&
            currentSetting.authToken == setting.authToken &&
            currentSetting.targetPeerId == setting.targetPeerId &&
            current.state.value is P2PConnectionState.Connected
        ) {
            return
        }

        if (current != null) {
            disconnect()
        }

        val clientId = setting.clientId.ifEmpty { "gogo-${UUID.randomUUID().toString().take(8)}" }

        val signaling = HttpSignalingClient(
            client = this.client,
            hubBaseUrl = setting.hubUrl,
            rootToken = setting.authToken,
            clientId = clientId,
        )
        signalingClient = signaling

        val newWebrtc = WebRTCClient(context, signaling, scope)
        val newTransport = DataChannelTransport(newWebrtc)

        // Per-connection Job: cancel on disconnect so we don't leak a
        // state collector that lives longer than the WebRTC client.
        val stateJob = scope.launch {
            newWebrtc.state.collect { state ->
                _connectionState.value = state
                when (state) {
                    is P2PConnectionState.Failed -> {
                        lastFailedAt = System.currentTimeMillis()
                        connectedSetting = null
                        modelsCache.clear()
                        Log.w(TAG, "P2P connection failed: ${state.reason}")
                    }
                    is P2PConnectionState.Disconnected -> {
                        connectedSetting = null
                        modelsCache.clear()
                        Log.i(TAG, "P2P disconnected")
                    }
                    else -> Unit
                }
            }
        }
        connectionJob = stateJob

        // webrtc.connect() can throw (signaling failure, ICE failure, etc).
        // On any failure, tear down half-built state so the next call starts
        // clean instead of reusing a dead transport.
        try {
            val token = signaling.refreshJwt()
            newWebrtc.connect(setting.targetPeerId, clientId, cachedIceServers, token.jwt)
            // Publish only after a successful connect so concurrent callers
            // never see a half-initialized transport.
            webrtcClient = newWebrtc
            transport = newTransport
            connectedSetting = setting
            Log.i(TAG, "P2P connected to ${setting.targetPeerId}")
        } catch (e: Exception) {
            Log.e(TAG, "ensureConnected failed; cleaning half-built state", e)
            stateJob.cancel()
            transport = null
            webrtcClient = null
            signalingClient = null
            connectedSetting = null
            lastFailedAt = System.currentTimeMillis()
            _connectionState.value = P2PConnectionState.Failed(e.message ?: "connect failed")
            throw e
        }
    }

    fun disconnect() {
        connectionJob?.cancel()
        connectionJob = null
        webrtcClient?.disconnect()
        webrtcClient = null
        transport = null
        signalingClient = null
        connectedSetting = null
        modelsCache.clear()
        // Keep lastFailedAt so the next ensureConnected respects the cooldown.
        _connectionState.value = P2PConnectionState.Disconnected
    }

    override fun close() {
        Log.i(TAG, "close() — releasing provider resources")
        disconnect()
        scope.cancel()
    }
}
