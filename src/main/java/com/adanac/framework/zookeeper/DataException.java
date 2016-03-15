package com.adanac.framework.zookeeper;

public class DataException extends RuntimeException {
	public DataException() {
		super();
	}

	public DataException(String message) {
		super(message);
	}

	public DataException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataException(Throwable cause) {
		super(cause);
	}
}
