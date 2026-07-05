package me.rerere.ai.provider.providers.sakerp2p

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import me.rerere.ai.core.InputSchema
import me.rerere.ai.core.MessageRole
import me.rerere.ai.core.Tool
import me.rerere.ai.ui.MessageChunk
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessageChoice
import me.rerere.ai.ui.UIMessagePart
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Covers AG-UI SSE parsing, image/embedding payload parsing, and run-request
 * building for the Saker P2P provider. The codec is a pure object with no
 * Android dependencies, so these run as plain JUnit tests on the JVM.
 */
class SakerP2PSseCodecTest {

    private val json = Json { ignoreUnknownKeys = true }

    // ==================== parseSSEPayload ====================

    @Test
    fun `text message content emits Text part with delta`() {
        val payload = "event: text_message_content\ndata: {\"delta\": \"Hello\"}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)

        assertNotNull(chunk)
        val delta = chunk!!.choices.first().delta
        assertEquals(MessageRole.ASSISTANT, delta?.role)
        val textPart = delta?.parts?.firstOrNull() as? UIMessagePart.Text
        assertNotNull(textPart)
        assertEquals("Hello", textPart?.text)
    }

    @Test
    fun `reasoning message content emits Reasoning part with delta`() {
        val payload = "event: reasoning_message_content\ndata: {\"delta\": \"thinking\"}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)

        assertNotNull(chunk)
        val reasoningPart = chunk!!.choices.first().delta?.parts?.firstOrNull() as? UIMessagePart.Reasoning
        assertNotNull(reasoningPart)
        assertEquals("thinking", reasoningPart?.reasoning)
    }

    @Test
    fun `tool call start emits Tool part with id and name`() {
        val payload = "event: tool_call_start\ndata: {\"tool_call_id\": \"call_42\", \"tool_name\": \"search\"}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)

        assertNotNull(chunk)
        val toolPart = chunk!!.choices.first().delta?.parts?.firstOrNull() as? UIMessagePart.Tool
        assertNotNull(toolPart)
        assertEquals("call_42", toolPart?.toolCallId)
        assertEquals("search", toolPart?.toolName)
        assertEquals("", toolPart?.input)
    }

    @Test
    fun `tool call args emits Tool part with delta as input`() {
        val payload = "event: tool_call_args\ndata: {\"tool_call_id\": \"call_42\", \"delta\": \"{\\\"q\\\": \\\"hi\\\"}\"}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)

        assertNotNull(chunk)
        val toolPart = chunk!!.choices.first().delta?.parts?.firstOrNull() as? UIMessagePart.Tool
        assertNotNull(toolPart)
        assertEquals("call_42", toolPart?.toolCallId)
        assertEquals("{\"q\": \"hi\"}", toolPart?.input)
    }

    @Test
    fun `tool call end returns null (completion marker)`() {
        val payload = "event: tool_call_end\ndata: {\"tool_call_id\": \"call_42\"}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)
        assertNull(chunk)
    }

    @Test
    fun `run finished emits stop finish reason`() {
        val payload = "event: run_finished\ndata: {}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)

        assertNotNull(chunk)
        assertNull(chunk!!.choices.first().delta)
        assertEquals("stop", chunk.choices.first().finishReason)
    }

    @Test
    fun `event type is case insensitive`() {
        val payload = "event: Text_Message_Content\ndata: {\"delta\": \"Hi\"}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)
        assertNotNull(chunk)
        val textPart = chunk!!.choices.first().delta?.parts?.firstOrNull() as? UIMessagePart.Text
        assertEquals("Hi", textPart?.text)
    }

    @Test
    fun `unknown event type returns null`() {
        val payload = "event: custom_event\ndata: {\"foo\": \"bar\"}\n\n"
        assertNull(SakerP2PSseCodec.parseSSEPayload(payload))
    }

    @Test
    fun `payload without event line returns null`() {
        val payload = "data: {\"delta\": \"Hello\"}\n\n"
        assertNull(SakerP2PSseCodec.parseSSEPayload(payload))
    }

    @Test
    fun `missing delta field emits empty text chunk`() {
        val payload = "event: text_message_content\ndata: {}\n\n"
        val chunk = SakerP2PSseCodec.parseSSEPayload(payload)
        assertNotNull(chunk)
        val delta = chunk!!.choices.first().delta
        // Empty text yields no parts (codec avoids emitting empty Text parts).
        assertTrue(delta?.parts.isNullOrEmpty())
    }

    // ==================== parseImageSSEPayload ====================

    @Test
    fun `image payload returns ImageGenerationItem with base64 and mime`() {
        val payload = "event: image\ndata: {\"image\": \"iVBOR==\", \"mime_type\": \"image/png\"}\n\n"
        val item = SakerP2PSseCodec.parseImageSSEPayload(payload)

        assertNotNull(item)
        assertEquals("iVBOR==", item!!.data)
        assertEquals("image/png", item.mimeType)
        assertEquals(false, item.partial)
        assertNull(item.partialImageIndex)
    }

    @Test
    fun `image payload with partial fields maps through`() {
        val payload = "event: image\ndata: {\"image\": \"abc\", \"mime_type\": \"image/jpeg\", \"partial\": true, \"partial_index\": 1}\n\n"
        val item = SakerP2PSseCodec.parseImageSSEPayload(payload)

        assertNotNull(item)
        assertEquals("abc", item!!.data)
        assertEquals("image/jpeg", item.mimeType)
        assertTrue(item.partial)
        assertEquals(1, item.partialImageIndex)
    }

    @Test
    fun `image payload defaults mime_type to png when missing`() {
        val payload = "data: {\"image\": \"abc\"}\n\n"
        val item = SakerP2PSseCodec.parseImageSSEPayload(payload)
        assertNotNull(item)
        assertEquals("image/png", item!!.mimeType)
    }

    @Test
    fun `image payload missing image field returns null`() {
        val payload = "data: {\"mime_type\": \"image/png\"}\n\n"
        assertNull(SakerP2PSseCodec.parseImageSSEPayload(payload))
    }

    @Test
    fun `image payload missing data line returns null`() {
        val payload = "event: image\n\n"
        assertNull(SakerP2PSseCodec.parseImageSSEPayload(payload))
    }

    @Test
    fun `image payload with malformed json returns null`() {
        val payload = "data: {not json}\n\n"
        assertNull(SakerP2PSseCodec.parseImageSSEPayload(payload))
    }

    // ==================== parseEmbeddingSSEPayload ====================

    @Test
    fun `embedding payload returns model and float vectors`() {
        val payload = "data: {\"model\": \"bge-small\", \"embeddings\": [[0.1, 0.2], [0.3, 0.4]]}\n\n"
        val result = SakerP2PSseCodec.parseEmbeddingSSEPayload(payload)

        assertNotNull(result)
        assertEquals("bge-small", result!!.model)
        assertEquals(2, result.embeddings.size)
        assertEquals(listOf(0.1f, 0.2f), result.embeddings[0])
        assertEquals(listOf(0.3f, 0.4f), result.embeddings[1])
    }

    @Test
    fun `embedding payload with empty embeddings array returns null`() {
        val payload = "data: {\"model\": \"x\", \"embeddings\": []}\n\n"
        assertNull(SakerP2PSseCodec.parseEmbeddingSSEPayload(payload))
    }

    @Test
    fun `embedding payload missing embeddings field returns null`() {
        val payload = "data: {\"model\": \"x\"}\n\n"
        assertNull(SakerP2PSseCodec.parseEmbeddingSSEPayload(payload))
    }

    @Test
    fun `embedding payload missing data line returns null`() {
        val payload = "event: embedding\n\n"
        assertNull(SakerP2PSseCodec.parseEmbeddingSSEPayload(payload))
    }

    // ==================== buildAGUIRunRequest ====================

    @Test
    fun `run request includes thread id and message roles`() {
        val messages = listOf(
            UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text("hi"))),
            UIMessage(role = MessageRole.ASSISTANT, parts = listOf(UIMessagePart.Text("hello"))),
        )
        val body = SakerP2PSseCodec.buildAGUIRunRequest(messages, "thread-1", emptyList())

        assertEquals("thread-1", body["threadId"]?.jsonPrimitive?.content)
        val messagesArr = body["messages"]?.jsonArray
        assertNotNull(messagesArr)
        assertEquals(2, messagesArr?.size)
        assertEquals("user", messagesArr?.get(0)?.jsonObject?.get("role")?.jsonPrimitive?.content)
        assertEquals("hi", messagesArr?.get(0)?.jsonObject?.get("content")?.jsonPrimitive?.content)
        assertEquals("assistant", messagesArr?.get(1)?.jsonObject?.get("role")?.jsonPrimitive?.content)
    }

    @Test
    fun `run request without tools omits tools field`() {
        val messages = listOf(UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text("hi"))))
        val body = SakerP2PSseCodec.buildAGUIRunRequest(messages, "t", emptyList())
        assertNull(body["tools"])
    }

    @Test
    fun `run request with tools serializes name description and parameters`() {
        val tool = Tool(
            name = "search",
            description = "Search the web",
            parameters = {
                InputSchema.Obj(
                    properties = buildJsonObject {
                        putJsonArray("query") { /* schema placeholder */ }
                    },
                    required = listOf("query"),
                )
            },
            execute = { emptyList() },
        )
        val messages = listOf(UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text("q"))))
        val body = SakerP2PSseCodec.buildAGUIRunRequest(messages, "t", listOf(tool))

        val toolsArr = body["tools"]?.jsonArray
        assertNotNull(toolsArr)
        assertEquals(1, toolsArr?.size)
        val toolObj = toolsArr?.get(0)?.jsonObject ?: error("tool object missing")
        assertEquals("search", toolObj["name"]?.jsonPrimitive?.content)
        assertEquals("Search the web", toolObj["description"]?.jsonPrimitive?.content)
        val params = toolObj["parameters"]?.jsonObject
        assertNotNull(params)
        assertEquals("object", params?.get("type")?.jsonPrimitive?.contentOrNull)
        assertEquals("query", params?.get("required")?.jsonArray?.firstOrNull()?.jsonPrimitive?.content)
    }

    @Test
    fun `run request with tool returning null parameters omits parameters field`() {
        val tool = Tool(
            name = "noop",
            description = "no params",
            parameters = { null },
            execute = { emptyList() },
        )
        val messages = listOf(UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text("q"))))
        val body = SakerP2PSseCodec.buildAGUIRunRequest(messages, "t", listOf(tool))

        val toolObj = body["tools"]?.jsonArray?.first()?.jsonObject ?: error("tool missing")
        assertEquals("noop", toolObj["name"]?.jsonPrimitive?.content)
        assertNull(toolObj["parameters"])
    }

    @Test
    fun `run request content joins text parts only`() {
        val messages = listOf(
            UIMessage(
                role = MessageRole.USER,
                parts = listOf(
                    UIMessagePart.Text("hello "),
                    UIMessagePart.Image(url = "ignored"),
                    UIMessagePart.Text("world"),
                )
            )
        )
        val body = SakerP2PSseCodec.buildAGUIRunRequest(messages, "t", emptyList())
        val content = body["messages"]?.jsonArray?.first()?.jsonObject?.get("content")?.jsonPrimitive?.content
        assertEquals("hello world", content)
    }

    // ==================== collectStreamToMessageChunk ====================

    private fun chunk(
        parts: List<UIMessagePart> = emptyList(),
        finishReason: String? = null,
    ) = MessageChunk(
        id = "test",
        model = "",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = if (parts.isEmpty() && finishReason == null) null
                    else UIMessage(role = MessageRole.ASSISTANT, parts = parts),
                message = null,
                finishReason = finishReason,
            )
        ),
        usage = null,
    )

    @Test
    fun `collect merges text deltas into ordered parts`() = runBlocking {
        val stream = flowOf(
            chunk(parts = listOf(UIMessagePart.Text("Hello"))),
            chunk(parts = listOf(UIMessagePart.Text(" world"))),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "test-model")

        assertEquals("test-model", result.model)
        assertTrue(result.id.startsWith("p2p-"))
        val choice = result.choices.first()
        assertNull(choice.delta)
        assertEquals(MessageRole.ASSISTANT, choice.message?.role)
        assertEquals(2, choice.message?.parts?.size)
        assertEquals("Hello", (choice.message?.parts?.get(0) as? UIMessagePart.Text)?.text)
        assertEquals(" world", (choice.message?.parts?.get(1) as? UIMessagePart.Text)?.text)
        assertEquals("stop", choice.finishReason)
    }

    @Test
    fun `collect preserves reasoning and text ordering`() = runBlocking {
        val stream = flowOf(
            chunk(parts = listOf(UIMessagePart.Reasoning(reasoning = "thinking"))),
            chunk(parts = listOf(UIMessagePart.Text("answer"))),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "m")

        val parts = result.choices.first().message?.parts
        assertEquals(2, parts?.size)
        assertTrue(parts?.get(0) is UIMessagePart.Reasoning)
        assertTrue(parts?.get(1) is UIMessagePart.Text)
    }

    @Test
    fun `collect merges tool call start and args as separate parts`() = runBlocking {
        val stream = flowOf(
            chunk(parts = listOf(UIMessagePart.Tool(toolCallId = "call_1", toolName = "search", input = ""))),
            chunk(parts = listOf(UIMessagePart.Tool(toolCallId = "call_1", toolName = "", input = "{\"q\":\"hi\"}"))),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "m")

        val parts = result.choices.first().message?.parts
        assertEquals(2, parts?.size)
        val first = parts?.get(0) as? UIMessagePart.Tool
        val second = parts?.get(1) as? UIMessagePart.Tool
        assertEquals("call_1", first?.toolCallId)
        assertEquals("search", first?.toolName)
        assertEquals("", first?.input)
        assertEquals("call_1", second?.toolCallId)
        assertEquals("{\"q\":\"hi\"}", second?.input)
    }

    @Test
    fun `collect carries finish reason from run finished chunk`() = runBlocking {
        val stream = flowOf(
            chunk(parts = listOf(UIMessagePart.Text("done"))),
            chunk(finishReason = "stop"),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "m")

        val choice = result.choices.first()
        assertEquals(1, choice.message?.parts?.size)
        assertNull(choice.delta)
        assertEquals("stop", choice.finishReason)
    }

    @Test
    fun `collect defaults finish reason to stop when stream has none`() = runBlocking {
        val stream = flowOf(
            chunk(parts = listOf(UIMessagePart.Text("no finish"))),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "m")

        assertEquals("stop", result.choices.first().finishReason)
    }

    @Test
    fun `collect on empty stream yields empty parts with stop reason`() = runBlocking {
        val result = SakerP2PSseCodec.collectStreamToMessageChunk(flowOf(), "m")

        val choice = result.choices.first()
        assertTrue(choice.message?.parts.isNullOrEmpty())
        assertEquals("stop", choice.finishReason)
    }

    @Test
    fun `collect skips chunks without choices`() = runBlocking {
        val emptyChoiceChunk = MessageChunk(
            id = "test",
            model = "",
            choices = emptyList(),
            usage = null,
        )
        val stream = flowOf(
            emptyChoiceChunk,
            chunk(parts = listOf(UIMessagePart.Text("after empty"))),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "m")

        val parts = result.choices.first().message?.parts
        assertEquals(1, parts?.size)
        assertEquals("after empty", (parts?.get(0) as? UIMessagePart.Text)?.text)
    }

    @Test
    fun `collect takes last non-null finish reason`() = runBlocking {
        val stream = flowOf(
            chunk(parts = listOf(UIMessagePart.Text("a")), finishReason = "length"),
            chunk(finishReason = "stop"),
        )

        val result = SakerP2PSseCodec.collectStreamToMessageChunk(stream, "m")

        assertEquals("stop", result.choices.first().finishReason)
    }
}
