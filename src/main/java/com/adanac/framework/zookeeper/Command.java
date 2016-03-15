package com.adanac.framework.zookeeper;

/**
 * An interface that captures a unit of work.
 *
 * @param <E> The type of exception that the command throws.
 */
public interface Command<E extends RuntimeException> {

	/**
	 * Performs a unit of work, possibly throwing {@code E} in the process.
	 *
	 * @throws E if there was a problem performing the work
	 */
	void execute() throws E;
}