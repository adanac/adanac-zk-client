package com.adanac.framework.zookeeper;

import com.adanac.framework.zookeeper.intf.BackoffStrategy;

/**
 * A BackoffStrategy that implements truncated binary exponential backoff.
 * @author adanac
 * @version 1.0
 */
public class TruncatedBinaryBackoff implements BackoffStrategy {
	private final long initialBackoffMs;

	private final long maxBackoffIntervalMs;

	private final boolean stopAtMax;

	private volatile boolean stop = false;

	/**
	 * Creates a new TruncatedBinaryBackoff that will start by backing off for {@code initialBackoff}
	 * and then backoff of twice as long each time its called until reaching the {@code maxBackoff} at
	 * which point shouldContinue() will return false and any future backoffs will always wait for
	 * that amount of time.
	 *
	 * @param initialBackoffMs the initial amount of time to backoff
	 * @param maxBackoffMs     the maximum amount of time to backoff
	 * @param stopAtMax        whether shouldContinue() returns false when the max is reached
	 */
	public TruncatedBinaryBackoff(long initialBackoffMs, long maxBackoffMs, boolean stopAtMax) {
		if (initialBackoffMs <= 0 || maxBackoffMs <= 0 || initialBackoffMs >= maxBackoffMs)
			throw new IllegalArgumentException();
		this.initialBackoffMs = initialBackoffMs;
		this.maxBackoffIntervalMs = maxBackoffMs;
		this.stopAtMax = stopAtMax;
	}

	/**
	 * Same as main constructor, but this will always return true from shouldContinue().
	 *
	 * @param initialBackoffMs the intial amount of time to backoff
	 * @param maxBackoffMs     the maximum amount of time to backoff
	 */
	public TruncatedBinaryBackoff(long initialBackoffMs, long maxBackoffMs) {
		this(initialBackoffMs, maxBackoffMs, false);
	}

	public long calculateBackoffMs(long lastBackoffMs) {
		if (lastBackoffMs < 0)
			throw new IllegalArgumentException();
		long backoff = (lastBackoffMs == 0) ? initialBackoffMs : Math.min(maxBackoffIntervalMs, lastBackoffMs * 2);
		stop = stop || (stopAtMax && (backoff >= maxBackoffIntervalMs));
		return backoff;
	}

	public boolean shouldContinue() {
		return !stop;
	}
}
