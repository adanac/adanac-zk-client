package com.adanac.framework.zookeeper.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adanac.framework.zookeeper.TruncatedBinaryBackoff;
import com.adanac.framework.zookeeper.intf.BackoffStrategy;
import com.adanac.framework.zookeeper.intf.Supplier;

/**
 * A utility for dealing with backoff of retryable actions.
 * @author adanac
 * @version 1.0
 */
public class BackoffHelper {
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private static final long DEFAULT_INITIAL_BACKOFF = 1L * 1000;// 1sec

	private static final long DEFAULT_MAX_BACKOFF = 1L * 60 * 1000;// 1min

	private final BackoffStrategy backoffStrategy;

	/**
	 * Creates a new BackoffHelper that uses truncated binary backoff starting at a 1 second backoff
	 * and maxing out at a 1 minute backoff.
	 */
	public BackoffHelper() {
		this(DEFAULT_INITIAL_BACKOFF, DEFAULT_MAX_BACKOFF);
	}

	/**
	 * Creates a new BackoffHelper that uses truncated binary backoff starting at the given
	 * {@code initialBackoff} and maxing out at the given {@code maxBackoff}.
	 *
	 * @param initialBackoffMs the initial amount of time to back off
	 * @param maxBackoffMs     the maximum amount of time to back off
	 */
	public BackoffHelper(long initialBackoffMs, long maxBackoffMs) {
		this(new TruncatedBinaryBackoff(initialBackoffMs, maxBackoffMs));
	}

	/**
	 * Creates a new BackoffHelper that uses truncated binary backoff starting at the given
	 * {@code initialBackoff} and maxing out at the given {@code maxBackoff}. This will either:
	 * <ul>
	 * <li>{@code stopAtMax == true} : throw {@code BackoffExpiredException} when maxBackoff is
	 * reached</li>
	 * <li>{@code stopAtMax == false} : continue backing off with maxBackoff</li>
	 * </ul>
	 *
	 * @param initialBackoffMs the initial amount of time to back off
	 * @param maxBackoffMs     the maximum amount of time to back off
	 * @param stopAtMax        if true, this will throw {@code BackoffStoppedException} when the max backoff is
	 *                         reached
	 */
	public BackoffHelper(long initialBackoffMs, long maxBackoffMs, boolean stopAtMax) {
		this(new TruncatedBinaryBackoff(initialBackoffMs, maxBackoffMs, stopAtMax));
	}

	/**
	 * Creates a BackoffHelper that uses the given {@code backoffStrategy} to calculate backoffs
	 * between retries.
	 *
	 * @param backoffStrategy the backoff strategy to use
	 */
	public BackoffHelper(BackoffStrategy backoffStrategy) {
		if (backoffStrategy == null)
			throw new IllegalArgumentException();
		this.backoffStrategy = backoffStrategy;
	}

	/**
	 * Executes the given task using the configured backoff strategy until the task succeeds as
	 * indicated by returning {@code true}.
	 *
	 * @param task the retryable task to execute until success
	 * @throws InterruptedException    if interrupted while waiting for the task to execute successfully
	 * @throws BackoffStoppedException if the backoff stopped unsuccessfully
	 * @throws E                       if the task throws
	 */
	public <E extends Exception> void doUntilSuccess(final Supplier<Boolean, E> task)
			throws InterruptedException, BackoffStoppedException, E {
		doUntilResult(new Supplier<Boolean, E>() {
			public Boolean get() throws E {
				Boolean result = task.get();
				return Boolean.TRUE.equals(result) ? result : null;
			}
		});
	}

	/**
	 * Executes the given task using the configured backoff strategy until the task succeeds as
	 * indicated by returning a non-null value.
	 *
	 * @param task the retryable task to execute until success
	 * @return the result of the successfully executed task
	 * @throws InterruptedException    if interrupted while waiting for the task to execute successfully
	 * @throws BackoffStoppedException if the backoff stopped unsuccessfully
	 * @throws E                       if the task throws
	 */
	public <T, E extends Exception> T doUntilResult(Supplier<T, E> task)
			throws InterruptedException, BackoffStoppedException, E {
		T result = task.get(); // give an immediate try
		return (result != null) ? result : retryWork(task);
	}

	private <T, E extends Exception> T retryWork(Supplier<T, E> work)
			throws E, InterruptedException, BackoffStoppedException {
		long currentBackoffMs = 0;
		while (backoffStrategy.shouldContinue()) {
			currentBackoffMs = backoffStrategy.calculateBackoffMs(currentBackoffMs);
			logger.info("Operation failed, backing off for " + currentBackoffMs + "ms");
			Thread.sleep(currentBackoffMs);
			T result = work.get();
			if (result != null) {
				return result;
			}
		}
		throw new BackoffStoppedException(String.format("Backoff stopped without succeeding."));
	}

	/**
	 * Occurs after the backoff strategy should stop.
	 */
	public static class BackoffStoppedException extends RuntimeException {

		private static final long serialVersionUID = 1413540048465041479L;

		public BackoffStoppedException(String msg) {
			super(msg);
		}
	}
}
