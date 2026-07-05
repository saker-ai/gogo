package me.rerere.p2p

import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Covers parsing of the GET /api/v1/signal/peers response body.
 *
 * The network layer is mocked away by calling [parsePeersResponseBody]
 * directly with canned JSON strings.
 */
class HttpSignalingClientPeersParseTest {

    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `empty peers array returns empty list`() {
        val body = """{"peers": []}"""
        val peers = parsePeersResponseBody(json, body)
        assertTrue(peers.isEmpty())
    }

    @Test
    fun `single peer with all fields maps correctly`() {
        val body = """
            {"peers": [{
                "peer_id": "p1",
                "instance_id": "i1",
                "models": ["llama", "qwen"],
                "labels": {"region": "us-east"},
                "version": "1.2.3"
            }]}
        """.trimIndent()
        val peers = parsePeersResponseBody(json, body)
        assertEquals(1, peers.size)
        val p = peers.first()
        assertEquals("p1", p.peerId)
        assertEquals("i1", p.instanceId)
        assertEquals(listOf("llama", "qwen"), p.models)
        assertEquals(mapOf("region" to "us-east"), p.labels)
        assertEquals("1.2.3", p.version)
    }

    @Test
    fun `multiple peers preserve order`() {
        val body = """
            {"peers": [
                {"peer_id": "a", "instance_id": "ia"},
                {"peer_id": "b", "instance_id": "ib"},
                {"peer_id": "c", "instance_id": "ic"}
            ]}
        """.trimIndent()
        val peers = parsePeersResponseBody(json, body)
        assertEquals(3, peers.size)
        assertEquals("a", peers[0].peerId)
        assertEquals("b", peers[1].peerId)
        assertEquals("c", peers[2].peerId)
    }

    @Test
    fun `peer with missing optional fields uses defaults`() {
        val body = """{"peers": [{"peer_id": "p1", "instance_id": "i1"}]}"""
        val peers = parsePeersResponseBody(json, body)
        assertEquals(1, peers.size)
        val p = peers.first()
        assertEquals("p1", p.peerId)
        assertEquals("i1", p.instanceId)
        assertTrue(p.models.isEmpty())
        assertTrue(p.labels.isEmpty())
        assertEquals("", p.version)
    }

    @Test
    fun `response without peers field returns empty list`() {
        val body = """{"other": "data"}"""
        val peers = parsePeersResponseBody(json, body)
        assertTrue(peers.isEmpty())
    }

    @Test
    fun `response with peers as non-array returns empty list`() {
        val body = """{"peers": "not an array"}"""
        val peers = parsePeersResponseBody(json, body)
        assertTrue(peers.isEmpty())
    }

    @Test
    fun `unknown fields in peer objects are ignored`() {
        val body = """
            {"peers": [{
                "peer_id": "p1",
                "instance_id": "i1",
                "future_field": "ignored"
            }]}
        """.trimIndent()
        val peers = parsePeersResponseBody(json, body)
        assertEquals(1, peers.size)
        assertEquals("p1", peers.first().peerId)
    }
}
