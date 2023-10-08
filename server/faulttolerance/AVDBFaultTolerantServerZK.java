package server.faulttolerance;

import client.AVDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import org.json.JSONException;
import org.json.JSONObject;
import server.AVDBReplicatedServer;
import server.AVDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * This class should implement your replicated fault-tolerant database server.
 * Refer to {@link ReplicatedServer} for a starting point.
 */
public class AVDBFaultTolerantServerZK extends AVDBSingleServer {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 200;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;


	final private Session session;
	final private Cluster cluster;

	protected final String myID;
	protected final MessageNIOTransport<String, String> serverMessenger;


	public AVDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);
		session = (cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(myID);
		log.log(Level.INFO, "Server {0} added cluster contact point", new
				Object[]{myID,});
		this.myID = myID;
		this.serverMessenger = new MessageNIOTransport<String, String>(myID,
				nodeConfig, new AbstractBytePacketDemultiplexer() {
			@Override
			public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
				handleMessageFromServer(bytes, nioHeader);
				return true;
			}
		}, true);

		log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this
				.myID, this.clientMessenger.getListeningSocketAddress()});
	}

	// TODO: process bytes received from clients here.
	// Currently this method simply invokes the SingleServer handler
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		super.handleMessageFromClient(bytes, header);
	}

	// TODO: process bytes received from fellow servers here.
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		throw new RuntimeException("Not implemented");
	}


	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new AVDBReplicatedServer(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], new InetSocketAddress
				("localhost", 9042));
	}

}