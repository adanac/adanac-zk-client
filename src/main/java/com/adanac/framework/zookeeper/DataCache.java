package com.adanac.framework.zookeeper;

public interface DataCache<T> {
	public T getData();

	public void sync(boolean retryUntilSuccess) throws DataException;

	public void monitor(T currentExpectData, DataListener<T> dataListener);

	public void destroy();
}
