package com.adanac.framework.zookeeper.intf;

/**
 * An interface that captures a source of data.
 * @param <T> The supplied value type.
 * @param <E> The type of exception that the supplier throws.
 * @author adanac
 * @version 1.0
 */
public interface Supplier<T, E extends Exception> {

	/**
	 * Supplies an item, possibly throwing {@code E} in the process of obtaining the item.
	 *
	 * @return the result of the computation
	 * @throws E if there was a problem performing the work
	 */
	T get() throws E;
}
