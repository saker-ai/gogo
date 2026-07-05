package me.rerere.p2p

import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Round-trip serialization tests for the §4.3 wire format envelopes:
 * AGUIFrame (application-layer), ChunkFrame (transport-layer fragmentation),
 * and P2PException (error frame). Verifies snake_case field mapping,
 * nullable defaults, unknown-field tolerance, and P2PException shape.
 */
class WireFormatSerializationTest {

    private val json = Json { ignoreUnknownKeys = true }

    // ==================== AGUIFrame ====================

    @Test
    fun `agui frame round trip preserves all fields`() {
        val original = AGUIFrame(
            type = "data",
            requestId = "req_123",
            payload = "hello",
            code = 200,
            message = "ok",
        )
        val encoded = json.encodeToString(AGUIFrame.serializer(), original)
        val decoded = json.decodeFromString(AGUIFrame.serializer(), encoded)
        assertEquals(original, decoded)
    }

    @Test
    fun `agui frame defaults nullable fields to null`() {
        val original = AGUIFrame(type = "auth")
        val encoded = json.encodeToString(AGUIFrame.serializer(), original)
        val decoded = json.decodeFromString(AGUIFrame.serializer(), encoded)
        assertEquals("auth", decoded.type)
        assertEquals(null, decoded.requestId)
        assertEquals(null, decoded.payload)
        assertEquals(null, decoded.code)
        assertEquals(null, decoded.message)
    }

    @Test
    fun `agui frame serializes request_id as snake_case`() {
        val frame = AGUIFrame(type = "data", requestId = "req_1")
        val encoded = json.encodeToString(AGUIFrame.serializer(), frame)
        assertTrue(encoded.contains("\"request_id\""))
        assertFalse(encoded.contains("\"requestId\""))
    }

    @Test
    fun `agui frame ignores unknown fields`() {
        val jsonStr = """{"type":"data","request_id":"req_1","payload":"hi","extra":"unknown"}"""
        val decoded = json.decodeFromString(AGUIFrame.serializer(), jsonStr)
        assertEquals("data", decoded.type)
        assertEquals("req_1", decoded.requestId)
        assertEquals("hi", decoded.payload)
    }

    // ==================== ChunkFrame ====================

    @Test
    fun `chunk frame round trip preserves all fields`() {
        val original = ChunkFrame(
            type = "chunk",
            requestId = "req_456",
            seq = 3,
            fin = true,
            data = "aGVsbG8=",
        )
        val encoded = json.encodeToString(ChunkFrame.serializer(), original)
        val decoded = json.decodeFromString(ChunkFrame.serializer(), encoded)
        assertEquals(original, decoded)
    }

    @Test
    fun `chunk frame serializes request_id as snake_case`() {
        val frame = ChunkFrame(
            type = "chunk",
            requestId = "req_2",
            seq = 0,
            fin = false,
            data = "",
        )
        val encoded = json.encodeToString(ChunkFrame.serializer(), frame)
        assertTrue(encoded.contains("\"request_id\""))
        assertFalse(encoded.contains("\"requestId\""))
    }

    @Test
    fun `chunk frame deserializes from wire format`() {
        val jsonStr = """{"type":"chunk","request_id":"req_3","seq":1,"fin":true,"data":"aGVsbG8="}"""
        val decoded = json.decodeFromString(ChunkFrame.serializer(), jsonStr)
        assertEquals("chunk", decoded.type)
        assertEquals("req_3", decoded.requestId)
        assertEquals(1, decoded.seq)
        assertEquals(true, decoded.fin)
        assertEquals("aGVsbG8=", decoded.data)
    }

    // ==================== P2PException ====================

    @Test
    fun `p2p exception preserves code and message`() {
        val ex = P2PException(code = 503, message = "service unavailable")
        assertEquals(503, ex.code)
        assertEquals("service unavailable", ex.message)
    }
}
