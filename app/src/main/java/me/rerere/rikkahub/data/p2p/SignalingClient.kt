package me.rerere.rikkahub.data.p2p

import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import org.webrtc.IceCandidate
import org.webrtc.SessionDescription

/**
 * HTTP signaling client for the Saker Hub (§3.3.3, §4.2 of the design doc).
 *
 * Endpoints:
 *   POST /api/v1/auth/token       — exchange root token for short-lived JWT
 *   POST /api/v1/signal/offer     — send SDP Offer, receive Answer + ICE servers
 *   POST /api/v1/signal/ice       — send local ICE candidate
 *   GET  /api/v1/signal/events    — SSE stream of remote ICE candidates
 *   GET  /api/v1/signal/peers     — list online Saker nodes
 */
interface SignalingClient {
    /**
     * Exchange the root token for a short-lived JWT (§7.2.1).
     */
    suspend fun exchangeToken(rootToken: String, clientId: String): AuthTokenResponse

    /**
     * Send the SDP Offer to the Hub, which forwards it to the target Saker node
     * over libp2p and returns the Answer plus session_id and ICE servers.
     */
    suspend fun sendOffer(
        targetPeerId: String,
        clientId: String,
        sdp: SessionDescription,
    ): SignalResponse

    /**
     * Send a local ICE candidate to the Hub for relay to the Saker node.
     */
    suspend fun sendIceCandidate(sessionId: String, candidate: IceCandidate)

    /**
     * Open the SSE stream of remote ICE candidates and connection events.
     */
    fun events(sessionId: String): Flow<SignalEvent>

    /**
     * List Saker nodes currently registered with the Hub.
     */
    suspend fun listPeers(): List<PeerInfo>

    /**
     * Refresh the JWT ahead of its expiry (§7.2.2). Returns the new token.
     */
    suspend fun refreshJwt(): AuthTokenResponse
}

@Serializable
data class AuthTokenResponse(
    val jwt: String,
    @SerialName("expires_at")
    val expiresAt: String,
    @SerialName("refresh_after")
    val refreshAfter: String,
)

@Serializable
data class SignalResponse(
    @SerialName("session_id")
    val sessionId: String,
    val sdp: SessionDescriptionSDP,
    @SerialName("ice_servers")
    val iceServers: List<ICEServerConfig> = emptyList(),
)

@Serializable
data class SessionDescriptionSDP(
    val type: String,
    val sdp: String,
)

@Serializable
data class ICEServerConfig(
    val urls: List<String>,
    val username: String? = null,
    val credential: String? = null,
)

@Serializable
data class PeerInfo(
    @SerialName("peer_id")
    val peerId: String,
    @SerialName("instance_id")
    val instanceId: String,
    val models: List<String> = emptyList(),
    val labels: Map<String, String> = emptyMap(),
    val version: String = "",
)

sealed class SignalEvent {
    data class Ice(val candidate: JsonElement) : SignalEvent()
    data class Connected(val sessionId: String) : SignalEvent()
    data class Failed(val sessionId: String, val reason: String) : SignalEvent()
}
