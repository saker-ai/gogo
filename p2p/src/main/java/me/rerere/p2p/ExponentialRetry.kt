package me.rerere.p2p

import kotlinx.coroutines.delay
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Exponential backoff retry policy (§10.1): 500ms → 1s → 2s → 4s → 8s → 16s → 30s cap,
 * at most [maxAttempts] attempts.
 */
class ExponentialRetry(
    private val initial: Duration = 500.milliseconds,
    private val max: Duration = 30.seconds,
    private val factor: Double = 2.0,
    val maxAttempts: Int = 5,
) {
    suspend fun <T> retry(block: suspend (attempt: Int) -> T): T {
        var lastError: Throwable? = null
        for (attempt in 0 until maxAttempts) {
            try {
                return block(attempt)
            } catch (e: Throwable) {
                lastError = e
                if (attempt + 1 >= maxAttempts) break
                val multiplier = factor.pow(attempt)
                val backoffMs = (initial.inWholeMilliseconds * multiplier)
                    .coerceAtMost(max.inWholeMilliseconds.toDouble())
                delay(backoffMs.toLong().milliseconds)
            }
        }
        throw lastError ?: IllegalStateException("retry failed without error")
    }
}
