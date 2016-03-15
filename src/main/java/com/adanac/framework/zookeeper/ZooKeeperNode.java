package com.adanac.framework.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adanac.framework.zookeeper.intf.Supplier;
import com.adanac.framework.zookeeper.util.BackoffHelper;
import com.adanac.framework.zookeeper.util.ZooKeeperUtils;

public class ZooKeeperNode<T> implements DataCache<T> {

	private static Logger logger = LoggerFactory.getLogger(ZooKeeperNode.class);
	private ZooKeeperClient client;
	private String nodePath;
	private NodeDeserializer<T> deserializer;
	private BackoffHelper backoffHelper;
	private volatile T nodeData;
	private volatile DataListener<T> dataListener;
	private final Watcher nodeWatcher;
	private volatile boolean destroyed = false;
	private Command expirationHandler;

	public static <T> ZooKeeperNode<T> create(ZooKeeperClient zkClient, String nodePath,
			NodeDeserializer<T> deserializer) {
		return new ZooKeeperNode<T>(zkClient, nodePath, deserializer);
	}

	ZooKeeperNode(final ZooKeeperClient zkClient, String path, NodeDeserializer<T> deserializer) {
		if (zkClient == null)
			throw new IllegalArgumentException();
		if (path == null || "".equals(path.trim()))
			throw new IllegalArgumentException();
		if (deserializer == null)
			throw new IllegalArgumentException();
		this.client = zkClient;
		this.nodePath = path;
		this.deserializer = deserializer;
		backoffHelper = new BackoffHelper();
		destroyed = false;
		nodeData = null;
		nodeWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				logger.debug("Node " + nodePath + " ,event:" + event);
				if (destroyed) {
					logger.warn("Node " + nodePath + " has been destroyed, will ignore event " + event);
					return;
				}
				if (event.getState() == KeeperState.SyncConnected && (!Event.EventType.None.equals(event.getType()))) {
					addWatchBackgroundJob();
				}
			}
		};
		expirationHandler = new Command() {

			@Override
			public String toString() {
				return "ExpirationHandler@" + hashCode() + " of node " + nodePath;
			}

			@Override
			public void execute() {
				if (destroyed) {
					logger.warn("Node " + nodePath + " has been destroyed.");
					return;
				}
				try {
					watchDataNodeUntilSuccess();
				} catch (InterruptedException e) {
					logger.warn("Interrupted while trying to re-establish watch to " + nodePath, e);
					Thread.currentThread().interrupt();
				}
			}
		};
	}

	@Override
	public void sync(boolean retryUntilSuccess) throws DataException {
		client.registerExpirationHandler(expirationHandler);
		if (retryUntilSuccess) {
			try {
				watchDataNodeUntilSuccess();
			} catch (InterruptedException e) {
				logger.warn("Interrupted while trying to watch a data node " + nodePath, e);
				Thread.currentThread().interrupt();
			}
		} else {
			try {
				watchDataNode();
			} catch (Exception ex) {
				addWatchBackgroundJob();
				throw new DataException(ex);
			}
		}
	}

	@Override
	public T getData() {
		return nodeData;
	}

	public DataListener getDataListener() {
		return dataListener;
	}

	@Override
	public void monitor(final T currentExpectData, final DataListener<T> dataListener) {
		client.addBackgroundJob(new Runnable() {
			@Override
			public void run() {
				if (destroyed) {
					logger.warn("Node " + nodePath + " has been destroyed.");
					return;
				}
				try {
					watchDataNodeUntilSuccess();
					if (!_equals(currentExpectData, nodeData)) {
						dataListener.dataChanged(currentExpectData, nodeData);
					}
					ZooKeeperNode.this.dataListener = dataListener;
				} catch (InterruptedException e) {
					logger.warn("Interrupted while trying to watch a data node " + nodePath, e);
					Thread.currentThread().interrupt();
				}
			}
		});
	}

	private boolean _equals(T data1, T data2) {
		if (data1 == null) {
			if (data2 != null) {
				return false;
			}
		} else if (!data1.equals(data2)) {
			return false;
		}
		return true;
	}

	private void addWatchBackgroundJob() {
		if (destroyed) {
			logger.warn("Node " + nodePath + " has been destroyed.");
			return;
		}
		client.addBackgroundJob(new Runnable() {
			@Override
			public void run() {
				try {
					watchDataNodeUntilSuccess();
				} catch (InterruptedException e) {
					logger.warn("Interrupted while trying to watch a data node " + nodePath, e);
					Thread.currentThread().interrupt();
				}
			}
		});
	}

	public void destroy() {
		if (this.expirationHandler != null) {
			client.unRegisterExpirationHandler(this.expirationHandler);
		}
		this.destroyed = true;
		this.nodeData = null;
		this.client = null;
		this.deserializer = null;
		this.backoffHelper = null;
		this.dataListener = null;
	}

	synchronized void updateData(T newData) {
		T oldData = nodeData;
		nodeData = newData;
		if (dataListener != null && !_equals(oldData, nodeData)) {
			dataListener.dataChanged(oldData, nodeData);
		}
	}

	private void watchDataNodeUntilSuccess() throws InterruptedException {
		if (destroyed) {
			logger.warn("Node " + nodePath + " has been destroyed.");
			return;
		}
		backoffHelper.doUntilSuccess(new Supplier<Boolean, InterruptedException>() {
			@Override
			public Boolean get() throws InterruptedException {
				try {
					watchDataNode();
					return true;
				} catch (KeeperException e) {
					logger.info("Watch path " + nodePath + " KeeperException", e);
					return !ZooKeeperUtils.isRetryable(e);
				} catch (ZooKeeperClient.ZooKeeperConnectionException e) {
					logger.info("Watch path " + nodePath + " occur ZooKeeperConnectionException", e);
					return false;
				}
			}
		});
	}

	private synchronized void watchDataNode()
			throws InterruptedException, KeeperException, ZooKeeperClient.ZooKeeperConnectionException {
		if (destroyed) {
			logger.warn("Node " + nodePath + " has been destroyed.");
			return;
		}
		try {
			Stat stat = new Stat();
			byte[] rawData = client.get().getData(nodePath, nodeWatcher, stat);
			T newData = deserializer.deserialize(rawData);
			updateData(newData);
		} catch (KeeperException.NoNodeException e) {
			updateData(null);
			if (!destroyed) {
				// This node doesn't exist right now, reflect that locally
				// and then create a watch to wait for its recreation.
				client.get().exists(nodePath, nodeWatcher);
			}
		}
	}
}
