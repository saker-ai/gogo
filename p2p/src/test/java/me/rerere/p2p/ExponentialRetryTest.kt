package me.rerere.p2p

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Duration.Companion.milliseconds

/**
 * Covers ExponentialRetry (§10.1): exponential backoff between attempts,
 * max attempts boundary, and cancellation propagation.
 *
 * Uses small initial/max durations so the suite runs fast without
 * VirtualTime support — the actual backoff durations are not asserted,
 * only the retry count, attempt sequence, and final outcome.
 */
class ExponentialRetryTest {

    @Test
    fun `first attempt success returns immediately`() = runBlocking {
        val retry = ExponentialRetry(maxAttempts = 5)
        val result = retry.retry { attempt ->
            assertEquals(0, attempt)
            "ok"
        }
        assertEquals("ok", result)
    }

    @Test
    fun `retries on failure and succeeds on later attempt`() = runBlocking {
        val retry = ExponentialRetry(
            initial = 1.milliseconds,
            max = 5.milliseconds,
            maxAttempts = 5,
        )
        val result = retry.retry { attempt ->
            if (attempt < 2) throw RuntimeException("fail")
            "ok-$attempt"
        }
        assertEquals("ok-2", result)
    }

    @Test
    fun `throws last error after all attempts fail`() {
        val retry = ExponentialRetry(
            initial = 1.milliseconds,
            max = 5.milliseconds,
            maxAttempts = 3,
        )
        val ex = assertThrows(RuntimeException::class.java) {
            runBlocking {
                retry.retry { throw RuntimeException("always fail") }
            }
        }
        assertEquals("always fail", ex.message)
    }

    @Test
    fun `block receives incrementing attempt index`() {
        val retry = ExponentialRetry(
            initial = 1.milliseconds,
            max = 5.milliseconds,
            maxAttempts = 5,
        )
        val attempts = mutableListOf<Int>()
        try {
            runBlocking {
                retry.retry { attempt ->
                    attempts.add(attempt)
                    throw RuntimeException("fail")
                }
            }
        } catch (e: RuntimeException) {
            // expected
        }
        assertEquals(listOf(0, 1, 2, 3, 4), attempts)
    }

    @Test
    fun `maxAttempts=1 means no retry`() {
        val retry = ExponentialRetry(maxAttempts = 1)
        val ex = assertThrows(RuntimeException::class.java) {
            runBlocking {
                retry.retry { throw RuntimeException("nope") }
            }
        }
        assertEquals("nope", ex.message)
    }

    @Test
    fun `custom factor succeeds with fewer attempts`() = runBlocking {
        // factor=3.0 grows backoff faster; still succeeds on attempt 3
        val retry = ExponentialRetry(
            initial = 1.milliseconds,
            max = 10.milliseconds,
            factor = 3.0,
            maxAttempts = 5,
        )
        val result = retry.retry { attempt ->
            if (attempt < 3) throw RuntimeException("fail")
            "ok-$attempt"
        }
        assertEquals("ok-3", result)
    }

    @Test
    fun `does not swallow cancellation exception`() {
        val retry = ExponentialRetry(
            initial = 1.milliseconds,
            max = 5.milliseconds,
            maxAttempts = 5,
        )
        val ex = assertThrows(CancellationException::class.java) {
            runBlocking {
                retry.retry { throw CancellationException("cancelled") }
            }
        }
        assertTrue(ex.message?.contains("cancelled") == true)
    }
}
