package me.rerere.rikkahub.data.p2p

/**
 * Connection state machine for the Saker P2P transport.
 *
 * Mirrors the libp2p/WebRTC connection lifecycle (§6.4 of the design doc):
 * - Disconnected → Connecting → Connected (DataChannel open + authed)
 * - Connecting → Reconnecting (ICE restart / network change)
 * - Any → Failed (terminal until user retries)
 */
sealed class P2PConnectionState {
    object Disconnected : P2PConnectionState()
    object Connecting : P2PConnectionState()
    object Connected : P2PConnectionState()
    object Reconnecting : P2PConnectionState()
    data class Failed(val reason: String) : P2PConnectionState()
}
