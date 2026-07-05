package me.rerere.ai.provider.providers.sakerp2p

import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import me.rerere.ai.core.InputSchema
import me.rerere.ai.core.MessageRole
import me.rerere.ai.core.Tool
import me.rerere.ai.provider.EmbeddingGenerationResult
import me.rerere.ai.ui.ImageGenerationItem
import me.rerere.ai.ui.MessageChunk
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessageChoice
import me.rerere.ai.ui.UIMessagePart
import java.util.UUID

/**
 * Pure codec for the Saker P2P AG-UI wire format. Extracted from
 * [SakerP2PProvider] so it can be unit-tested without instantiating
 * WebRTC/OkHttp/Context dependencies.
 *
 * Parsing is intentionally lenient: malformed payloads return null and the
 * caller silently drops them, since P2P frames are best-effort.
 */
internal object SakerP2PSseCodec {
    private val json = Json { ignoreUnknownKeys = true }

    fun buildAGUIRunRequest(
        messages: List<UIMessage>,
        threadId: String,
        tools: List<Tool>,
    ): JsonObject = buildJsonObject {
        put("threadId", threadId)
        putJsonArray("messages") {
            for (m in messages) {
                add(buildJsonObject {
                    put("role", m.role.name.lowercase())
                    put("content", m.contentText())
                })
            }
        }
        if (tools.isNotEmpty()) {
            putJsonArray("tools") {
                for (tool in tools) {
                    add(buildJsonObject {
                        put("name", tool.name)
                        put("description", tool.description)
                        val params = tool.parameters()
                        if (params != null) {
                            put("parameters", json.encodeToJsonElement(InputSchema.serializer(), params))
                        }
                    })
                }
            }
        }
    }

    fun parseSSEPayload(payload: String): MessageChunk? {
        val lines = payload.lines()
        val eventType = lines
            .firstOrNull { it.startsWith("event: ") }
            ?.removePrefix("event: ")
            ?.trim()
            ?.uppercase()
            ?: return null
        val dataLine = lines.firstOrNull { it.startsWith("data: ") }?.removePrefix("data: ")?.trim()
        val data = if (dataLine.isNullOrEmpty()) null else json.parseToJsonElement(dataLine) as? JsonObject

        return when (eventType) {
            "TEXT_MESSAGE_CONTENT" -> textChunk(data?.get("delta")?.jsonPrimitive?.contentOrNull ?: "")
            "REASONING_MESSAGE_CONTENT" -> reasoningChunk(data?.get("delta")?.jsonPrimitive?.contentOrNull ?: "")
            "TOOL_CALL_START" -> {
                val toolCallId = data?.get("tool_call_id")?.jsonPrimitive?.contentOrNull ?: ""
                val toolName = data?.get("tool_name")?.jsonPrimitive?.contentOrNull ?: ""
                toolCallStartChunk(toolCallId, toolName)
            }
            "TOOL_CALL_ARGS" -> {
                val toolCallId = data?.get("tool_call_id")?.jsonPrimitive?.contentOrNull ?: ""
                val delta = data?.get("delta")?.jsonPrimitive?.contentOrNull ?: ""
                toolCallArgsChunk(toolCallId, delta)
            }
            "TOOL_CALL_END" -> null  // completion marker; no part emitted
            "RUN_FINISHED" -> finishChunk()
            else -> null
        }
    }

    fun parseImageSSEPayload(payload: String): ImageGenerationItem? {
        val lines = payload.lines()
        val dataLine = lines.firstOrNull { it.startsWith("data: ") }?.removePrefix("data: ")?.trim() ?: return null
        if (dataLine.isEmpty()) return null
        return try {
            val obj = json.parseToJsonElement(dataLine) as? JsonObject ?: return null
            val base64 = obj["image"]?.jsonPrimitive?.contentOrNull ?: return null
            val mimeType = obj["mime_type"]?.jsonPrimitive?.contentOrNull ?: "image/png"
            val partial = obj["partial"]?.jsonPrimitive?.booleanOrNull ?: false
            val partialIndex = obj["partial_index"]?.jsonPrimitive?.intOrNull
            ImageGenerationItem(
                data = base64,
                mimeType = mimeType,
                partial = partial,
                partialImageIndex = partialIndex,
            )
        } catch (e: Exception) {
            null
        }
    }

    fun parseEmbeddingSSEPayload(payload: String): EmbeddingGenerationResult? {
        val lines = payload.lines()
        val dataLine = lines.firstOrNull { it.startsWith("data: ") }?.removePrefix("data: ")?.trim() ?: return null
        if (dataLine.isEmpty()) return null
        return try {
            val obj = json.parseToJsonElement(dataLine) as? JsonObject ?: return null
            val model = obj["model"]?.jsonPrimitive?.contentOrNull ?: ""
            val embeddingsArr = obj["embeddings"] as? JsonArray ?: return null
            val embeddings = embeddingsArr.mapNotNull { vec ->
                val vecArr = vec as? JsonArray ?: return@mapNotNull null
                vecArr.mapNotNull { it.jsonPrimitive.doubleOrNull?.toFloat() }
            }
            if (embeddings.isEmpty()) return null
            EmbeddingGenerationResult(model = model, embeddings = embeddings)
        } catch (e: Exception) {
            null
        }
    }

    /**
     * Aggregate a stream of [MessageChunk]s into a single non-streaming
     * [MessageChunk] for callers of `generateText`. Preserves every part
     * emitted across the stream (text + reasoning + tool calls) in order,
     * and carries forward the last non-null finishReason (defaulting to
     * "stop" if the stream completed without one).
     *
     * Extracted as a pure function so the merge logic can be unit-tested
     * without a live WebRTC transport.
     */
    suspend fun collectStreamToMessageChunk(
        stream: Flow<MessageChunk>,
        modelId: String,
    ): MessageChunk {
        val parts = mutableListOf<UIMessagePart>()
        var finishReason: String? = null
        stream.collect { chunk ->
            val choice = chunk.choices.firstOrNull() ?: return@collect
            choice.delta?.parts?.let { parts.addAll(it) }
            choice.finishReason?.let { finishReason = it }
        }
        return MessageChunk(
            id = "p2p-${UUID.randomUUID().toString().take(8)}",
            model = modelId,
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

    private fun textChunk(text: String): MessageChunk = MessageChunk(
        id = "p2p-stream",
        model = "",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = if (text.isEmpty()) emptyList() else listOf(UIMessagePart.Text(text = text)),
                ),
                message = null,
                finishReason = null,
            )
        ),
        usage = null,
    )

    private fun reasoningChunk(text: String): MessageChunk = MessageChunk(
        id = "p2p-stream",
        model = "",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = if (text.isEmpty()) emptyList() else listOf(UIMessagePart.Reasoning(reasoning = text)),
                ),
                message = null,
                finishReason = null,
            )
        ),
        usage = null,
    )

    private fun toolCallStartChunk(toolCallId: String, toolName: String): MessageChunk = MessageChunk(
        id = "p2p-stream",
        model = "",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = listOf(
                        UIMessagePart.Tool(
                            toolCallId = toolCallId,
                            toolName = toolName,
                            input = "",
                        )
                    ),
                ),
                message = null,
                finishReason = null,
            )
        ),
        usage = null,
    )

    private fun toolCallArgsChunk(toolCallId: String, delta: String): MessageChunk = MessageChunk(
        id = "p2p-stream",
        model = "",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = listOf(
                        UIMessagePart.Tool(
                            toolCallId = toolCallId,
                            toolName = "",
                            input = delta,
                        )
                    ),
                ),
                message = null,
                finishReason = null,
            )
        ),
        usage = null,
    )

    private fun finishChunk(): MessageChunk = MessageChunk(
        id = "p2p-stream",
        model = "",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = null,
                message = null,
                finishReason = "stop",
            )
        ),
        usage = null,
    )
}

private fun UIMessage.contentText(): String {
    return parts.joinToString("") { part ->
        when (part) {
            is UIMessagePart.Text -> part.text
            else -> ""
        }
    }
}
