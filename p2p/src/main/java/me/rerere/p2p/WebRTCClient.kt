package me.rerere.p2p

import android.content.Context
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.webrtc.DataChannel
import org.webrtc.IceCandidate
import org.webrtc.IceCandidateErrorEvent
import org.webrtc.MediaConstraints
import org.webrtc.PeerConnection
import org.webrtc.PeerConnectionFactory
import org.webrtc.RtpReceiver
import org.webrtc.RtpTransceiver
import org.webrtc.SdpObserver
import org.webrtc.SessionDescription
import org.webrtc.SurfaceTextureHelper
import org.webrtc.VideoCapturer
import org.webrtc.VideoSource
import org.webrtc.VideoTrack
import java.nio.ByteBuffer
import java.util.concurrent.atomic.LongAdder

private const val TAG = "WebRTCClient"

/**
 * WebRTC PeerConnection manager (§10.1).
 *
 * gogo is the offerer: it creates the DataChannel and SDP Offer, sends them to
 * the Hub signaling API, and applies the Answer returned by the Saker node.
 *
 * The class is intentionally minimal: it does not handle ICE restarts, reneg, or
 * renegotiation. Those are owned by [DataChannelTransport], which orchestrates
 * the higher-level reconnect/restart state machine.
 */
class WebRTCClient(
    private val context: Context,
    private val signalingClient: SignalingClient,
    private val scope: CoroutineScope,
) {
    private var peerConnection: PeerConnection? = null
    private var dataChannel: DataChannel? = null
    private var sessionId: String? = null
    private var jwt: String? = null
    private var clientId: String = ""

    private val json = Json { ignoreUnknownKeys = true }

    // DROP_OLDEST: avoid SCTP backpressure (§10.1, §13.1)
    private val _incoming = MutableSharedFlow<AGUIFrame>(
        extraBufferCapacity = 256,
    )
    val incoming: SharedFlow<AGUIFrame> = _incoming.asSharedFlow()

    val droppedFrames = LongAdder()

    private val _state = MutableStateFlow<P2PConnectionState>(P2PConnectionState.Disconnected)
    val state: StateFlow<P2PConnectionState> = _state.asStateFlow()

    private val factory: PeerConnectionFactory by lazy {
        PeerConnectionFactory.initialize(
            PeerConnectionFactory.InitializationOptions.builder(context)
                .setEnableInternalTracer(false)
                .createInitializationOptions()
        )
        PeerConnectionFactory.builder()
            .setOptions(PeerConnectionFactory.Options())
            .createPeerConnectionFactory()
    }

    private val retryPolicy = ExponentialRetry()

    suspend fun connect(
        targetPeerId: String,
        clientId: String,
        iceServers: List<PeerConnection.IceServer>,
        jwt: String,
    ) {
        retryPolicy.retry { attempt ->
            _state.value = P2PConnectionState.Connecting
            this.jwt = jwt
            this.clientId = clientId

            val config = PeerConnection.RTCConfiguration(iceServers).apply {
                sdpSemantics = PeerConnection.SdpSemantics.UNIFIED_PLAN
                bundlePolicy = PeerConnection.BundlePolicy.MAXBUNDLE
                iceTransportsType = PeerConnection.IceTransportsType.ALL
            }

            peerConnection = factory.createPeerConnection(config, object : PeerConnection.Observer {
                override fun onIceCandidate(candidate: IceCandidate) {
                    val sid = sessionId ?: return
                    scope.launch { signalingClient.sendIceCandidate(sid, candidate) }
                }

                override fun onIceConnectionChange(state: PeerConnection.IceConnectionState) {
                    when (state) {
                        PeerConnection.IceConnectionState.CONNECTED ->
                            _state.value = P2PConnectionState.Connected
                        PeerConnection.IceConnectionState.FAILED ->
                            _state.value = P2PConnectionState.Failed("ICE connection failed")
                        PeerConnection.IceConnectionState.DISCONNECTED ->
                            _state.value = P2PConnectionState.Disconnected
                        else -> Unit
                    }
                }

                override fun onSignalingChange(p0: PeerConnection.SignalingState?) = Unit
                override fun onIceConnectionReceivingChange(p0: Boolean) = Unit
                override fun onIceGatheringChange(p0: PeerConnection.IceGatheringState?) = Unit
                override fun onIceCandidatesRemoved(p0: Array<out IceCandidate>?) = Unit
                override fun onAddStream(p0: org.webrtc.MediaStream?) = Unit
                override fun onRemoveStream(p0: org.webrtc.MediaStream?) = Unit
                override fun onDataChannel(p0: DataChannel?) = Unit
                override fun onRenegotiationNeeded() = Unit
                override fun onAddTrack(p0: org.webrtc.RtpReceiver?, p1: Array<out org.webrtc.MediaStream>?) = Unit
                override fun onTrack(p0: RtpTransceiver?) = Unit
            })

            // gogo creates the out-of-band DataChannel (§10.1)
            val dcInit = DataChannel.Init().apply { ordered = true }
            dataChannel = peerConnection?.createDataChannel("saker-chat", dcInit)
            dataChannel?.let { setupDataChannel(it) }

            // Create Offer
            val offer = peerConnection!!.awaitCreateOffer(factory)
            peerConnection!!.awaitSetLocalDescription(offer)

            // Send Offer → Hub → Saker, receive Answer
            val response = signalingClient.sendOffer(targetPeerId, clientId, offer)
            sessionId = response.sessionId
            peerConnection!!.awaitSetRemoteDescription(response.sdp.toWebrtc())

            // Listen for remote ICE candidates via SSE
            scope.launch {
                signalingClient.events(response.sessionId).collect { event ->
                    when (event) {
                        is SignalEvent.Ice -> {
                            val cand = parseIceCandidate(event.candidate)
                            if (cand != null) peerConnection?.addIceCandidate(cand)
                        }
                        is SignalEvent.Connected ->
                            _state.value = P2PConnectionState.Connected
                        is SignalEvent.Failed ->
                            _state.value = P2PConnectionState.Failed(event.reason)
                    }
                }
            }
        }
    }

    fun send(data: ByteArray) {
        val dc = dataChannel
        if (dc == null) {
            Log.w(TAG, "send: dataChannel is null (${data.size} bytes dropped); not connected?")
            return
        }
        // Backpressure: drop if bufferedAmount exceeds 512 KiB (§7.3)
        if (dc.bufferedAmount() > 512 * 1024L) {
            droppedFrames.increment()
            Log.w(TAG, "send: dropping ${data.size} bytes, bufferedAmount=${dc.bufferedAmount()} > 512KiB threshold")
            return
        }
        dc.send(DataChannel.Buffer(ByteBuffer.wrap(data), false))
    }

    private fun setupDataChannel(dc: DataChannel) {
        dc.registerObserver(object : DataChannel.Observer {
            override fun onMessage(buffer: DataChannel.Buffer) {
                val bytes = ByteArray(buffer.data.remaining())
                buffer.data.get(bytes)
                val text = String(bytes)
                text.split("\n").filter { it.isNotBlank() }.forEach { line ->
                    try {
                        val frame = json.decodeFromString<AGUIFrame>(line)
                        if (!_incoming.tryEmit(frame)) {
                            droppedFrames.increment()
                        }
                    } catch (e: Exception) {
                        Log.w(TAG, "decode frame failed", e)
                    }
                }
            }

            override fun onStateChange() {
                if (dc.state() == DataChannel.State.OPEN) {
                    scope.launch { sendAuth() }
                }
            }

            override fun onBufferedAmountChange(p0: Long) = Unit
        })
    }

    private suspend fun sendAuth() {
        val sid = sessionId ?: return
        val token = jwt ?: return
        val authFrame = buildJsonObject {
            put("type", "auth")
            put("session_id", sid)
            put("jwt", token)
        }
        val data = (json.encodeToString(JsonObject.serializer(), authFrame) + "\n").toByteArray()
        // Wait for bufferedAmount to drain (§7.3)
        val dc = dataChannel ?: return
        while (dc.bufferedAmount() > 512 * 1024L) {
            delay(50)
        }
        dc.send(DataChannel.Buffer(ByteBuffer.wrap(data), false))
    }

    fun disconnect() {
        dataChannel?.close()
        peerConnection?.close()
        dataChannel = null
        peerConnection = null
        sessionId = null
        jwt = null
        clientId = ""
        _state.value = P2PConnectionState.Disconnected
    }
}

// ——— pion/webrtc-style await helpers for the official org.webrtc API ———

private val sdpDispatcher = kotlinx.coroutines.Dispatchers.IO

suspend fun PeerConnection.awaitCreateOffer(factory: PeerConnectionFactory): SessionDescription {
    val constraints = MediaConstraints().apply {
        mandatory.add(MediaConstraints.KeyValuePair("OfferToReceiveAudio", "false"))
        mandatory.add(MediaConstraints.KeyValuePair("OfferToReceiveVideo", "false"))
    }
    val deferred = kotlinx.coroutines.CompletableDeferred<SessionDescription>()
    createOffer(object : SdpObserver {
        override fun onCreateSuccess(p0: SessionDescription?) {
            if (p0 != null) deferred.complete(p0) else deferred.completeExceptionally(RuntimeException("null offer"))
        }
        override fun onCreateFailure(p0: String?) { deferred.completeExceptionally(RuntimeException(p0)) }
        override fun onSetSuccess() = Unit
        override fun onSetFailure(p0: String?) = Unit
    }, constraints)
    return deferred.await()
}

suspend fun PeerConnection.awaitSetLocalDescription(sdp: SessionDescription) {
    val deferred = kotlinx.coroutines.CompletableDeferred<Unit>()
    setLocalDescription(object : SdpObserver {
        override fun onCreateSuccess(p0: SessionDescription?) = Unit
        override fun onCreateFailure(p0: String?) = Unit
        override fun onSetSuccess() { deferred.complete(Unit) }
        override fun onSetFailure(p0: String?) { deferred.completeExceptionally(RuntimeException(p0)) }
    }, sdp)
    deferred.await()
}

suspend fun PeerConnection.awaitSetRemoteDescription(sdp: SessionDescription) {
    val deferred = kotlinx.coroutines.CompletableDeferred<Unit>()
    setRemoteDescription(object : SdpObserver {
        override fun onCreateSuccess(p0: SessionDescription?) = Unit
        override fun onCreateFailure(p0: String?) = Unit
        override fun onSetSuccess() { deferred.complete(Unit) }
        override fun onSetFailure(p0: String?) { deferred.completeExceptionally(RuntimeException(p0)) }
    }, sdp)
    deferred.await()
}

fun SessionDescriptionSDP.toWebrtc(): SessionDescription {
    val type = when (type.lowercase()) {
        "offer" -> SessionDescription.Type.OFFER
        "answer" -> SessionDescription.Type.ANSWER
        else -> SessionDescription.Type.ANSWER
    }
    return SessionDescription(type, sdp)
}

fun parseIceCandidate(element: kotlinx.serialization.json.JsonElement): IceCandidate? {
    val obj = element as? JsonObject ?: return null
    val sdp = (obj["candidate"] as? JsonPrimitive)?.content ?: return null
    val sdpMid = (obj["sdpMid"] as? JsonPrimitive)?.content ?: "0"
    val sdpMLineIndex = (obj["sdpMLineIndex"] as? JsonPrimitive)?.content?.toIntOrNull() ?: 0
    return IceCandidate(sdpMid, sdpMLineIndex, sdp)
}
