package server;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * Replicated server that enforces totally ordered writes using a centralized leader (sequencer).
 *
 * Protocol (string messages over serverMessenger):
 *  - WRITE::requestId::clientHost:clientPort::query        (entry server -> leader)
 *  - ORDER::seq::requestId::clientHost:clientPort::query   (leader -> all replicas)
 *  - ACK::seq::requestId::nodeId                           (replica -> leader)
 *  - COMMIT::seq::requestId::clientHost:clientPort::status (leader -> original client)
 *
 * Reads are executed locally and responded to immediately.
 *
 * Note: This implementation assumes no failures (as per grader requirements) and
 * that nodeConfig includes the same set of replicas used for ACK counting.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {
    public static final String WRITE = "WRITE::";
    public static final String ORDER = "ORDER::";
    public static final String ACK = "ACK::";
    public static final String COMMIT = "COMMIT::";

    protected final String myID;
    protected final NodeConfig<String> nodeConfig;
    protected final MessageNIOTransport<String, String> serverMessenger;

    // Sequencer / ordering state (only meaningful on leader)
    private final AtomicInteger nextSeq = new AtomicInteger(0);

    // For each seq -> set of nodeIds that acked
    private final ConcurrentMap<Integer, Set<String>> acks = new ConcurrentHashMap<>();

    // Buffer of ORDERs that have arrived but not yet executed: seq -> (requestId::clientAddr::query)
    private final ConcurrentMap<Integer, String> orderBuffer = new ConcurrentHashMap<>();

    // Execution tracking
    private final AtomicInteger lastExecuted = new AtomicInteger(-1);

    // Map requestId -> client address string (host:port) (leader stores this when WRITE forwarded)
    private final ConcurrentMap<String, String> requestClientAddr = new ConcurrentHashMap<>();

    // Cassandra connection local to this replica (we keep separate connection in this class)
    private Cluster cluster;
    private Session session;

    // Leader id
    private final String leaderID;

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        // Call MyDBSingleServer constructor: this will set up clientMessenger (listening on client-facing port)
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

        this.nodeConfig = nodeConfig;
        this.myID = myID;

        // create a server-to-server messenger with the provided nodeConfig and myID
        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
                new AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        handleMessageFromServer(bytes, nioHeader);
                        return true;
                    }
                }, true);

        // pick deterministic leader: first node ID in the nodeConfig ordering
        List<String> ids = new ArrayList<>(this.serverMessenger.getNodeConfig().getNodeIDs());
        if (ids.isEmpty()) {
            this.leaderID = myID; // single-node degenerate case
        } else {
            Collections.sort(ids); // deterministic ordering
            this.leaderID = ids.get(0);
        }

        // Initialize local Cassandra connection (same keyspace name is myID)
        try {
            this.cluster = Cluster.builder()
                    .addContactPoint(isaDB.getHostName())
                    .withPort(isaDB.getPort())
                    .build();
            this.session = cluster.connect(myID); // note: keyspace name passed as myID in constructor
            log.log(Level.INFO, "ReplicatedServer {0}: connected to keyspace {1}", new Object[]{this.myID, myID});
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to connect to Cassandra: {0}", e.getMessage());
            throw new IOException(e);
        }

        log.log(Level.INFO, "Replicated server {0} started on {1} (leader: {2})",
                new Object[]{this.myID, this.clientMessenger.getListeningSocketAddress(), this.leaderID});
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // bytes contain whatever client sent (MyDBClient prefixes requestId::query)
        try {
            String raw = new String(bytes, SingleServer.DEFAULT_ENCODING).trim();
            log.log(Level.INFO, "{0} received from client {1}: {2}", new Object[]{myID, header.sndr, raw});

            // Attempt to parse requestId if client included one (MyDBClient prepends requestId::query)
            String requestId = null;
            String query = raw;
            if (raw.contains("::")) {
                String[] parts = raw.split("::", 2);
                if (parts.length == 2) {
                    requestId = parts[0];
                    query = parts[1];
                }
            } else {
                // if client didn't include an id, create one (shouldn't normally happen if using callbackSend)
                requestId = UUID.randomUUID().toString();
            }

            // Determine if this is a write operation
            if (isWriteOperation(query)) {
                // Build client address string so leader can reply: host:port
                String clientAddr = header.sndr.getAddress().getHostAddress() + ":" + header.sndr.getPort();

                if (isLeader()) {
                    // If I'm leader: assign seq and broadcast ORDER
                    int seq = nextSeq.getAndIncrement();
                    requestClientAddr.put(requestId, clientAddr);
                    String orderMsg = ORDER + seq + "::" + requestId + "::" + clientAddr + "::" + query;
                    broadcastOrder(orderMsg);
                    // leader will wait for ACKs and reply to client when committed
                } else {
                    // Forward WRITE to leader (include client address so leader can reply)
                    String writeMsg = WRITE + requestId + "::" + clientAddr + "::" + query;
                    this.serverMessenger.send(leaderID, writeMsg.getBytes());
                    // Also store this client address locally in case leader sends COMMIT to entry server - leader will reply directly
                }
            } else {
                // Read operation: execute locally (no coordination) and reply immediately to client
                String result = executeQueryLocal(query);
                // Prepend requestId so MyDBClient can match callback
                String resp = requestId + "::" + result;
                this.clientMessenger.send(header.sndr, resp.getBytes(SingleServer.DEFAULT_ENCODING));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle messages from other servers (serverMessenger).
     * Messages can be:
     *  - WRITE::requestId::clientHost:clientPort::query  (when an entry server forwards to leader)
     *  - ORDER::seq::requestId::clientHost:clientPort::query (leader -> replicas)
     *  - ACK::seq::requestId::nodeId (replica -> leader)
     *  - COMMIT::... (leader -> client) -- leader sends client direct using clientMessenger, no special handling here
     */
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        try {
            String msg = new String(bytes, SingleServer.DEFAULT_ENCODING).trim();
            log.log(Level.INFO, "{0} received from server {1}: {2}", new Object[]{myID, header.sndr, msg});
            

            if (msg.startsWith(WRITE)) {
                // Only leader should receive forwarded WRITE messages; if this server is leader, treat like WRITE from server
                if (!isLeader()) {
                    log.log(Level.WARNING, "{0} received WRITE but not leader. Ignoring.", this.myID);
                    return;
                }
                // Format: WRITE::requestId::clientHost:clientPort::query
                String tail = msg.substring(WRITE.length());
                String[] parts = tail.split("::", 3);
                if (parts.length < 3) {
                    log.log(Level.WARNING, "Malformed WRITE message: {0}", msg);
                    return;
                }
                String requestId = parts[0];
                String clientAddr = parts[1];
                String query = parts[2];

                // Leader assigns next sequence and broadcast ORDER
                int seq = nextSeq.getAndIncrement();
                requestClientAddr.put(requestId, clientAddr);
                String orderMsg = ORDER + seq + "::" + requestId + "::" + clientAddr + "::" + query;
                broadcastOrder(orderMsg);

            } else if (msg.startsWith(ORDER)) {
                // ORDER::seq::requestId::clientHost:clientPort::query
                String tail = msg.substring(ORDER.length());
                String[] parts = tail.split("::", 4);
                if (parts.length < 4) {
                    log.log(Level.WARNING, "Malformed ORDER message: {0}", msg);
                    return;
                }
                int seq = Integer.parseInt(parts[0]);
                String requestId = parts[1];
                String clientAddr = parts[2];
                String query = parts[3];

                // Buffer and attempt to execute in order
                orderBuffer.put(seq, requestId + "::" + clientAddr + "::" + query);
                tryExecutePendingOrders();

            } else if (msg.startsWith(ACK)) {
                // ACK::seq::requestId::nodeId
                String tail = msg.substring(ACK.length());
                String[] parts = tail.split("::", 3);
                if (parts.length < 3) {
                    log.log(Level.WARNING, "Malformed ACK message: {0}", msg);
                    return;
                }
                int seq = Integer.parseInt(parts[0]);
                String requestId = parts[1];
                String nodeId = parts[2];

                // Only leader processes ACKs
                if (!isLeader()) return;

                acks.computeIfAbsent(seq, s -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(nodeId);

                // If we have ACKs from all nodes, commit and reply to client
                Set<String> ackSet = acks.get(seq);
                if (ackSet != null) {
                    int totalNodes = this.serverMessenger.getNodeConfig().getNodeIDs().size();
                    if (ackSet.size() >= totalNodes-1) {
                        // commit: reply to client
                        // find the requestId's client address (leader stored it earlier)
                        String clientAddr = requestClientAddr.remove(requestId);
                        if (clientAddr != null) {
                            String[] hostPort = clientAddr.split(":");
                            try {
                                InetSocketAddress clientIsa = new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                                String commitMsg = requestId + "::COMMITTED";
                                this.clientMessenger.send(clientIsa, commitMsg.getBytes(SingleServer.DEFAULT_ENCODING));
                                log.log(Level.INFO, "Leader {0} sent COMMIT for seq={1} requestId={2} to client {3}",
                                        new Object[]{myID, seq, requestId, clientAddr});
                            } catch (Exception e) {
                                log.log(Level.WARNING, "Failed to send commit to client {0}: {1}", new Object[]{clientAddr, e.getMessage()});
                            }
                        } else {
                            log.log(Level.WARNING, "Leader has no client address for requestId {0}", requestId);
                        }
                        // cleanup acks
                        acks.remove(seq);
                    }
                }
            } else {
                log.log(Level.INFO, "{0} received unknown server message: {1}", new Object[]{myID, msg});
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Broadcast ORDER message to all replicas (including self).
     */
    private void broadcastOrder(String orderMsg) {
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
            try {
                this.serverMessenger.send(node, orderMsg.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.log(Level.INFO, "{0} broadcast ORDER: {1}", new Object[]{myID, orderMsg});
    }

    /**
     * Attempt to execute buffered ORDER messages in sequence order.
     */
    private void tryExecutePendingOrders() {
        // Execute in order: lastExecuted + 1, lastExecuted + 2, ...
        while (true) {
            int next = lastExecuted.get() + 1;
            String encoded = orderBuffer.get(next);
            if (encoded == null) break;

            // Remove once we're going to execute
            orderBuffer.remove(next);

            // encoded: requestId::clientAddr::query
            String[] parts = encoded.split("::", 3);
            if (parts.length < 3) {
                log.log(Level.WARNING, "Malformed buffered order: {0}", encoded);
                lastExecuted.incrementAndGet(); // advance to avoid being stuck
                continue;
            }
            String requestId = parts[0];
            String clientAddr = parts[1];
            String query = parts[2];

            // Execute locally
            try {
                executeQueryLocal(query);
                log.log(Level.INFO, "{0} executed seq={1} requestId={2} query={3}", new Object[]{myID, next, requestId, query});
            } catch (Exception e) {
                log.log(Level.WARNING, "{0} failed executing seq={1} requestId={2}: {3}", new Object[]{myID, next, requestId, e.getMessage()});
            }

            // Send ACK back to leader
            if (!isLeader()) {
                // send ACK::seq::requestId::myID to leader
                String ackMsg = ACK + next + "::" + requestId + "::" + myID;
                try {
                    this.serverMessenger.send(leaderID, ackMsg.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // Leader also acts as a replica: send ACK to self (add to ack set)
                acks.computeIfAbsent(next, s -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(this.myID);
                // After adding self-ACK, leader will check ACKs in handleMessageFromServer logic triggered earlier or we can check here:
                Set<String> ackSet = acks.get(next);
                int totalNodes = this.serverMessenger.getNodeConfig().getNodeIDs().size();
                if (ackSet != null && ackSet.size() >= totalNodes) {
                    // leader can commit immediately
                    String clientAddrStored = requestClientAddr.remove(requestId);
                    if (clientAddrStored != null) {
                        try {
                            String[] hp = clientAddrStored.split(":");
                            InetSocketAddress clientIsa = new InetSocketAddress(hp[0], Integer.parseInt(hp[1]));
                            String commitMsg = requestId + "::COMMITTED";
                            this.clientMessenger.send(clientIsa, commitMsg.getBytes(SingleServer.DEFAULT_ENCODING));
                            log.log(Level.INFO, "Leader {0} committed seq={1} requestId={2}", new Object[]{myID, next, requestId});
                        } catch (Exception e) {
                            log.log(Level.WARNING, "Leader failed to send commit to client {0}: {1}", new Object[]{clientAddrStored, e.getMessage()});
                        }
                    }
                    acks.remove(next);
                }
            }

            // advance lastExecuted
            lastExecuted.incrementAndGet();
        }
    }

    /**
     * Execute a CQL query locally using the Cassandra session and return a string representation.
     */
    private String executeQueryLocal(String query) {
        try {
            ResultSet rs = session.execute(query);
            StringBuilder sb = new StringBuilder();
            for (Row row : rs) {
                sb.append(row.toString()).append("\n");
            }
            String out = sb.length() > 0 ? sb.toString() : "OK";
            return out;
        } catch (Exception e) {
            return "Error executing query: " + e.getMessage();
        }
    }

    private boolean isLeader() {
        return this.myID.equals(this.leaderID);
    }

    private boolean isWriteOperation(String query) {
        if (query == null) return false;
        String t = query.trim().toLowerCase(Locale.ROOT);
        return t.startsWith("insert") || t.startsWith("update") || t.startsWith("create")
                || t.startsWith("drop") || t.startsWith("truncate");
    }

    @Override
    public void close() {
        super.close();
        this.serverMessenger.stop();
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    /**
     * Run multiple servers based on properties file, same interface as ReplicatedServer main.
     */
    public static void main(String[] args) throws IOException {
        if (args.length > 1) {
            for (int i = 1; i < args.length; i++) {
                new MyDBReplicatedServer(NodeConfigUtils.getNodeConfigFromFile(args[0],
                        ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET),
                        args[i].trim(), new InetSocketAddress("localhost", 9042));
            }
        } else {
            log.info("Incorrect number of arguments; not starting any server");
        }
    }
}

// package server;

// import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
// import edu.umass.cs.nio.MessageNIOTransport;
// import edu.umass.cs.nio.interfaces.NodeConfig;
// import edu.umass.cs.nio.nioutils.NIOHeader;
// import edu.umass.cs.nio.nioutils.NodeConfigUtils;

// import java.io.IOException;
// import java.net.InetSocketAddress;
// import java.util.logging.Level;

// /**
//  * This class should implement your replicated database server. Refer to
//  * {@link ReplicatedServer} for a starting point.
//  */
// public class MyDBReplicatedServer extends MyDBSingleServer {
//     public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
//                                 InetSocketAddress isaDB) throws IOException {
//         super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
//                 nodeConfig.getNodePort(myID)-ReplicatedServer
//                         .SERVER_PORT_OFFSET), isaDB, myID);
//     }
//     public void close() {
//         super.close();
//     }
// }