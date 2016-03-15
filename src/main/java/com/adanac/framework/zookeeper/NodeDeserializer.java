package com.adanac.framework.zookeeper;

/**
 * Interface for defining zookeeper node data deserialization.
 *
 * @param <T> the type of data associated with this node
 * @author 
 */

public interface NodeDeserializer<T> {
	/**
	 * @param data the byte array returned from ZooKeeper when a watch is triggered.
	 */
	T deserialize(byte[] data);
}