package me.rerere.p2p

import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

/**
 * Covers the §4.3 chunked-frame reassembly logic extracted from
 * DataChannelTransport.chatStream. Each chunk's `data` is base64;
 * the transport concatenates across all chunks for a request_id and
 * decodes the final payload when `fin` is set.
 */
class DataChannelTransportChunkReassembleTest {

    private fun b64(s: String): String =
        Base64.getEncoder().encodeToString(s.toByteArray(Charsets.UTF_8))

    private fun newBuffers() = ConcurrentHashMap<String, StringBuilder>()

    private fun chunk(
        requestId: String,
        seq: Int,
        fin: Boolean,
        data: String,
    ) = ChunkFrame(
        type = "chunk",
        requestId = requestId,
        seq = seq,
        fin = fin,
        data = data,
    )

    @Test
    fun `single fin chunk decodes immediately`() {
        val buffers = newBuffers()
        val result = reassembleChunk(buffers, "req_1", chunk("req_1", 0, true, b64("hello")))
        assertEquals("hello", result)
        // Buffer is cleared after fin.
        assertNull(buffers["req_1"])
    }

    @Test
    fun `multiple chunks concatenate before fin`() {
        val buffers = newBuffers()
        // Split the base64 of "hello world" into two pieces. Each piece alone
        // isn't a valid base64 string, but concatenating them reconstructs the
        // full encoding — which is exactly the §4.3 wire contract.
        val fullB64 = b64("hello world")
        val splitAt = fullB64.length / 2
        var result = reassembleChunk(buffers, "req_2", chunk("req_2", 0, false, fullB64.substring(0, splitAt)))
        assertNull(result)
        result = reassembleChunk(buffers, "req_2", chunk("req_2", 1, true, fullB64.substring(splitAt)))
        assertEquals("hello world", result)
        assertNull(buffers["req_2"])
    }

    @Test
    fun `non-fin chunk returns null and keeps buffering`() {
        val buffers = newBuffers()
        val result = reassembleChunk(buffers, "req_3", chunk("req_3", 0, false, b64("partial")))
        assertNull(result)
        // Buffer still holds the partial data.
        assertEquals(b64("partial"), buffers["req_3"]?.toString())
    }

    @Test
    fun `multiple request ids keep independent buffers`() {
        val buffers = newBuffers()
        // Use substrings of a single base64 to respect the §4.3 wire contract
        // (chunk.data is a piece of one full encoding, not an independently
        // encoded string — otherwise `=` padding mid-stream breaks decode).
        val fullB64A = b64("a-end")
        val fullB64B = b64("b-data")
        val splitA = fullB64A.length / 2
        val splitB = fullB64B.length / 2
        reassembleChunk(buffers, "req_a", chunk("req_a", 0, false, fullB64A.substring(0, splitA)))
        reassembleChunk(buffers, "req_b", chunk("req_b", 0, false, fullB64B.substring(0, splitB)))
        assertEquals(fullB64A.substring(0, splitA), buffers["req_a"]?.toString())
        assertEquals(fullB64B.substring(0, splitB), buffers["req_b"]?.toString())
        // Finishing req_a doesn't touch req_b's buffer.
        val resultA = reassembleChunk(buffers, "req_a", chunk("req_a", 1, true, fullB64A.substring(splitA)))
        assertEquals("a-end", resultA)
        assertEquals(fullB64B.substring(0, splitB), buffers["req_b"]?.toString())
    }

    @Test
    fun `fin clears buffer so a new sequence with same request id starts fresh`() {
        val buffers = newBuffers()
        val fullB64 = b64("first")
        reassembleChunk(buffers, "req_x", chunk("req_x", 0, true, fullB64))
        // After fin, a new chunk with the same request_id should not contain
        // leftover data from the previous sequence.
        val secondB64 = b64("second")
        val result = reassembleChunk(buffers, "req_x", chunk("req_x", 0, true, secondB64))
        assertEquals("second", result)
    }

    @Test
    fun `malformed base64 on fin returns null`() {
        val buffers = newBuffers()
        // Accumulate a non-fin chunk with valid base64, then a fin chunk with
        // invalid base64. The concatenation isn't valid base64, so decode fails.
        reassembleChunk(buffers, "req_y", chunk("req_y", 0, false, "!!!not-base64!!!"))
        val result = reassembleChunk(buffers, "req_y", chunk("req_y", 1, true, "!!!more-garbage!!!"))
        assertNull(result)
        // Buffer is still cleared on fin even if decode failed.
        assertNull(buffers["req_y"])
    }

    @Test
    fun `empty data chunks decode to empty string on fin`() {
        val buffers = newBuffers()
        reassembleChunk(buffers, "req_z", chunk("req_z", 0, false, ""))
        val result = reassembleChunk(buffers, "req_z", chunk("req_z", 1, true, ""))
        assertEquals("", result)
        assertNull(buffers["req_z"])
    }
}
