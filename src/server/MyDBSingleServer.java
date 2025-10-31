package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.*;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {
    private Cluster cluster;
    private Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        // Initialize connection to Cassandra
        cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostName())
                .withPort(isaDB.getPort())
                .build();

        session = cluster.connect(keyspace);
        log.info("Connected to Cassandra keyspace: " + keyspace);
    }

    /**
     * Handle messages (queries) coming from clients.
     */
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
    try {
        String received = new String(bytes, DEFAULT_ENCODING).trim();

        // If request ID exists, split it
        String requestId = null;
        String query = received;

        if (received.contains("::")) {
            String[] parts = received.split("::", 2);
            requestId = parts[0];
            query = parts[1];
            System.out.println("Received requestId: " + requestId + " with query: " + query);
        }

        String response;
        try {
            ResultSet rs = session.execute(query);
            StringBuilder sb = new StringBuilder();
            for (Row row : rs) {
                sb.append(row.toString()).append("\n");
            }
            response = sb.length() > 0 ? sb.toString() : "Query executed successfully.";
        } catch (Exception e) {
            response = "Error executing query: " + e.getMessage();
        }

        // Prepend request ID back to response if it exists
        if (requestId != null) {
            response = requestId + "::" + response;
        }

        clientMessenger.send(header.sndr, response.getBytes(DEFAULT_ENCODING));

    } catch (IOException e) {
        e.printStackTrace();
        }
    }

    public void close() {
        super.close();
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    public static void main(String[] args) throws IOException {
        new MyDBSingleServer(
                getSocketAddress(args),
                new InetSocketAddress("localhost", 9042),
                "demo"
        );
    }
}