package me.rerere.ai.provider

import android.content.Context
import java.io.Closeable
import me.rerere.ai.provider.providers.ClaudeProvider
import me.rerere.ai.provider.providers.GoogleProvider
import me.rerere.ai.provider.providers.OpenAIProvider
import me.rerere.ai.provider.providers.sakerp2p.SakerP2PProvider
import okhttp3.OkHttpClient

/**
 * Provider管理器，负责注册和获取Provider实例
 *
 * 实现 [Closeable] 以便应用退出时释放底层资源（如 SakerP2PProvider 持有的
 * WebRTC PeerConnection 和协程 scope）。调用方应在 Application.onTerminate
 * 或等价生命周期钩子里调用 [close]。
 */
class ProviderManager(client: OkHttpClient, context: Context) : Closeable {
    // 存储已注册的Provider实例
    private val providers = mutableMapOf<String, Provider<*>>()

    init {
        // 注册默认Provider
        registerProvider("openai", OpenAIProvider(client, context))
        registerProvider("google", GoogleProvider(client, context))
        registerProvider("claude", ClaudeProvider(client, context))
        registerProvider("saker_p2p", SakerP2PProvider(client, context))
    }

    /**
     * 注册Provider实例
     *
     * @param name Provider名称
     * @param provider Provider实例
     */
    fun registerProvider(name: String, provider: Provider<*>) {
        providers[name] = provider
    }

    /**
     * 获取Provider实例
     *
     * @param name Provider名称
     * @return Provider实例
     */
    fun getProvider(name: String): Provider<*> {
        return providers[name] ?: throw IllegalArgumentException("Provider not found: $name")
    }

    /**
     * 根据ProviderSetting获取对应的Provider实例
     *
     * @param setting Provider设置
     * @return Provider实例
     */
    fun <T : ProviderSetting> getProviderByType(setting: T): Provider<T> {
        @Suppress("UNCHECKED_CAST")
        return when (setting) {
            is ProviderSetting.OpenAI -> getProvider("openai")
            is ProviderSetting.Google -> getProvider("google")
            is ProviderSetting.Claude -> getProvider("claude")
            is ProviderSetting.SakerP2P -> getProvider("saker_p2p")
        } as Provider<T>
    }

    /**
     * 释放所有 Provider 持有的底层资源。仅对实现了 [Closeable] 的 Provider
     * 生效（目前仅 SakerP2PProvider）；其他无状态 Provider 是空操作。
     */
    override fun close() {
        providers.values.forEach { provider ->
            (provider as? Closeable)?.close()
        }
    }
}
