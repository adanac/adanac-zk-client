package com.adanac.framework.zookeeper;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adanac.framework.statistics.VersionStatistics;

/**
 * zk 客户端
 * @author adanac
 * @version 1.0
 */
public class ZooKeeperClient {
	static {
		VersionStatistics.reportVersion(ZooKeeperClient.class);
	}

	public class ZooKeeperConnectionException extends Exception {

		private static final long serialVersionUID = -5677867348614801727L;

		public ZooKeeperConnectionException(Throwable cause) {
			super(cause);
		}

		public ZooKeeperConnectionException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	public interface Credentials {
		Credentials NONE = new Credentials() {

			@Override
			public void authenticate(ZooKeeper zooKeeper) {
				// noop
			}

			@Override
			public String scheme() {
				return null;
			}

			@Override
			public byte[] authToken() {
				return null;
			}

		};

		void authenticate(ZooKeeper zooKeeper);

		String scheme();

		byte[] authToken();
	}

	public static Credentials digestCredentials(String username, String password) {
		return credentials("digest", (username + ":" + password).getBytes());
	}

	public static Credentials credentials(final String scheme, final byte[] authToken) {
		return new Credentials() {
			@Override
			public void authenticate(ZooKeeper zooKeeper) {
				zooKeeper.addAuthInfo(scheme, authToken);
			}

			@Override
			public String scheme() {
				return scheme;
			}

			@Override
			public byte[] authToken() {
				return authToken;
			}
		};
	}

	private static Logger logger = LoggerFactory.getLogger(ZooKeeperClient.class);
	private static ConcurrentHashMap<String, ZooKeeperClient> clients = new ConcurrentHashMap<String, ZooKeeperClient>();
	private static final int DEFAULT_ZK_SESSION_TIMEOUT = 60000;
	private final int sessionTimeoutMs;
	private final Credentials credentials;
	private final String zooKeeperServers;
	private volatile ZooKeeper zooKeeper;
	private final Set<Command<?>> expirationHandlers = new CopyOnWriteArraySet<Command<?>>();
	private final BlockingQueue<Runnable> jobs = new LinkedBlockingQueue<Runnable>();
	private volatile boolean destroyed = false;
	private Thread backgroundProcessor = null;

	public ZooKeeperClient(int sessionTimeout, Credentials credentials, String zooKeeperServers) {
		if (sessionTimeout == 0 || credentials == null || zooKeeperServers == null)
			throw new IllegalArgumentException();
		// use dedicated thread to handler biz watcher,
		// let zookeeper event thread non-block(prevent thread is occupied, can
		// not handler session expired).
		backgroundProcessor = new Thread("ZookeeperClient-backgroundProcessor") {
			@Override
			public void run() {
				while (true) {
					if (destroyed) {
						break;
					}
					try {
						Runnable runnable = jobs.take();
						runnable.run();
					} catch (Throwable ex) {
						logger.error("Exception occur when process job.", ex);
					}
				}
			}
		};
		backgroundProcessor.setDaemon(true);
		backgroundProcessor.start();
		this.sessionTimeoutMs = sessionTimeout;
		this.credentials = credentials;
		this.zooKeeperServers = zooKeeperServers;
	}

	public ZooKeeperClient(int sessionTimeout, String zooKeeperServers) {
		this(sessionTimeout, Credentials.NONE, zooKeeperServers);
	}

	public ZooKeeperClient(String zooKeeperServers) {
		this(DEFAULT_ZK_SESSION_TIMEOUT, Credentials.NONE, zooKeeperServers);
	}

	public ZooKeeperClient(Credentials credentials, String zooKeeperServers) {
		this(DEFAULT_ZK_SESSION_TIMEOUT, credentials, zooKeeperServers);
	}

	public static ZooKeeperClient getInstance(String zooKeeperServers) {
		ZooKeeperClient client = new ZooKeeperClient(zooKeeperServers);
		ZooKeeperClient existed = clients.putIfAbsent(zooKeeperServers, client);
		return existed != null ? existed : client;
	}

	public static ZooKeeperClient getInstance(Credentials credentials, String zooKeeperServers) {
		ZooKeeperClient client = new ZooKeeperClient(credentials, zooKeeperServers);
		ZooKeeperClient existed = clients.putIfAbsent(zooKeeperServers, client);
		return existed != null ? existed : client;
	}

	public static void shutdown() {
		for (ZooKeeperClient client : clients.values()) {
			client.destroy();
		}
	}

	public void destroy() {
		destroyed = true;
		if (backgroundProcessor != null) {
			backgroundProcessor.interrupt();
		}
		close();
	}

	public boolean hasCredentials() {
		String scheme = credentials.scheme();
		return (scheme != null && !"".equals(scheme.trim())) && (credentials.authToken() != null);
	}

	public Watcher watcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			logger.debug("ZookeeperClient watcher, event:" + event);
			switch (event.getType()) {
			case None:
				switch (event.getState()) {
				case SyncConnected:
					logger.info("Zookeeper syncConnected. Event: " + event);
					break;
				case Disconnected:
					logger.info("Zookeeper Disconnected. Event: " + event);
					break;
				case Expired:
					logger.info("Zookeeper session expired. Event: " + event);
					close();
					addBackgroundJob(new Runnable() {
						@Override
						public void run() {
							executeExpirationHandlers();
						}
					});
					break;
				}
			}
		}
	};

	public synchronized ZooKeeper get() throws ZooKeeperConnectionException {
		if (zooKeeper != null) {
			return zooKeeper;
		}
		try {
			zooKeeper = new ZooKeeper(zooKeeperServers, sessionTimeoutMs, watcher);
			credentials.authenticate(zooKeeper);
			return zooKeeper;
		} catch (IOException ex) {
			throw new ZooKeeperConnectionException(ex);
		}
	}

	public synchronized void close() {
		if (zooKeeper != null) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Interrupted trying to close zooKeeper");
			} finally {
				zooKeeper = null;
				logger.info("Zookeeper has been closed.");
			}
		}
	}

	public void addBackgroundJob(Runnable job) {
		jobs.offer(job);
	}

	public void registerExpirationHandler(Command onExpired) {
		expirationHandlers.add(onExpired);
	}

	public void unRegisterExpirationHandler(Command onExpired) {
		expirationHandlers.remove(onExpired);
	}

	private void executeExpirationHandlers() {
		for (Command handler : expirationHandlers) {
			try {
				handler.execute();
				logger.info("Complete execute expirationHandler " + handler);
			} catch (Throwable ex) {
				logger.error("Exception occur when execute expirationHandler", ex);
			}
		}
		logger.info("Complete execute expirationHandlers.");
	}
}
