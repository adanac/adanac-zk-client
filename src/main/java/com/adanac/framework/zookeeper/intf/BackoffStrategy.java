package com.adanac.framework.zookeeper.intf;

/**
 * Encapsulates a strategy for backing off from an operation that repeatedly fails.
 *
 * @author 
 */

public interface BackoffStrategy {

	/**
	 * Calculates the amount of time to backoff from an operation.
	 *
	 * @param lastBackoffMs the last used backoff in milliseconds where 0 signifies no backoff has
	 *                      been performed yet
	 * @return the amount of time in milliseconds to back off before retrying the operation
	 */
	long calculateBackoffMs(long lastBackoffMs);

	/**
	 * Returns whether to continue backing off.
	 *
	 * @return whether to continue backing off
	 */
	boolean shouldContinue();
}