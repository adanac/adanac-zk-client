package com.adanac.framework.zookeeper;

public interface DataListener<T> {
	public void dataChanged(T oldData, T newData);
}
