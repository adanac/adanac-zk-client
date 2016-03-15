package com.adanac.framework.zookeeper.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adanac.framework.zookeeper.ZooKeeperClient;
import com.adanac.framework.zookeeper.ZooKeeperNode;

/**
 * Utilities for dealing with zoo keeper.
 * @author adanac
 * @version 1.0
 */
public class ZooKeeperUtils {
	private static Logger logger = LoggerFactory.getLogger(ZooKeeperNode.class);

	/**
	 * The magic version number that allows any mutation to always succeed regardless of actual
	 * version number.
	 */
	public static final int ANY_VERSION = -1;

	/**
	 * An ACL that gives all permissions to node creators and read permissions only to everyone else.
	 */
	public static final List<ACL> EVERYONE_READ_CREATOR_ALL = new ArrayList<ACL>();

	static {
		EVERYONE_READ_CREATOR_ALL.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
		EVERYONE_READ_CREATOR_ALL.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
	}

	/**
	 * Returns true if the given exception indicates an error that can be resolved by retrying the
	 * operation without modification.
	 *
	 * @param e the exception to check
	 * @return true if the causing operation is strictly retryable
	 */
	public static boolean isRetryable(KeeperException e) {
		switch (e.code()) {
		case CONNECTIONLOSS:
		case SESSIONEXPIRED:
		case SESSIONMOVED:
		case OPERATIONTIMEOUT:
			return true;
		case RUNTIMEINCONSISTENCY:
		case DATAINCONSISTENCY:
		case MARSHALLINGERROR:
		case BADARGUMENTS:
		case NONODE:
		case NOAUTH:
		case BADVERSION:
		case NOCHILDRENFOREPHEMERALS:
		case NODEEXISTS:
		case NOTEMPTY:
		case INVALIDCALLBACK:
		case INVALIDACL:
		case AUTHFAILED:
		case UNIMPLEMENTED:
			// These two should not be encountered - they are used internally by
			// ZK to specify ranges
		case SYSTEMERROR:
		case APIERROR:
		case OK: // This is actually an invalid ZK exception code
		default:
			return false;
		}
	}

	/**
	 * Ensures the given {@code path} exists in the ZK cluster accessed by {@code zkClient}. If the
	 * path already exists, nothing is done; however if any portion of the path is missing, it will be
	 * created with the given {@code acl} as a persistent zookeeper node. The given {@code path} must
	 * be a valid zookeeper absolute path.
	 *
	 * @param zkClient the client to use to access the ZK cluster
	 * @param acl      the acl to use if creating path nodes
	 * @param path     the path to ensure exists
	 * @throws ZooKeeperClient.ZooKeeperConnectionException if there was a problem accessing
	 *                                                      the ZK cluster
	 * @throws InterruptedException                         if we were interrupted
	 *                                                      attempting to connect to the ZK
	 *                                                      cluster
	 * @throws KeeperException                              if there was a problem in ZK
	 */
	public static void ensurePath(ZooKeeperClient zkClient, List<ACL> acl, String path)
			throws ZooKeeperClient.ZooKeeperConnectionException, InterruptedException, KeeperException {
		ensurePathInternal(zkClient, acl, path);
	}

	private static void ensurePathInternal(ZooKeeperClient zkClient, List<ACL> acl, String path)
			throws ZooKeeperClient.ZooKeeperConnectionException, InterruptedException, KeeperException {
		if (zkClient.get().exists(path, false) == null) {
			// The current path does not exist; so back up a level and ensure
			// the parent path exists
			// unless we're already a root-level path.
			int lastPathIndex = path.lastIndexOf('/');
			if (lastPathIndex > 0) {
				ensurePathInternal(zkClient, acl, path.substring(0, lastPathIndex));
			}
			// We've ensured our parent path (if any) exists so we can proceed
			// to create our path.
			try {
				zkClient.get().create(path, null, acl, CreateMode.PERSISTENT);
			} catch (KeeperException.NodeExistsException e) {
				// This ensures we don't die if a race condition was met between
				// checking existence and
				// trying to create the node.
				logger.info("Node existed when trying to ensure path " + path + ", somebody beat us to it?");
			}
		}
	}

	/**
	 * Validate and return a normalized zookeeper path which doesn't contain consecutive slashes and
	 * never ends with a slash (except for root path).
	 *
	 * @param path the path to be normalized
	 * @return normalized path string
	 */
	public static String normalizePath(String path) {
		String normalizedPath = path.replaceAll("//+", "/").replaceFirst("(.+)/$", "$1");
		PathUtils.validatePath(normalizedPath);
		return normalizedPath;
	}

	private ZooKeeperUtils() {
		// utility
	}
}
