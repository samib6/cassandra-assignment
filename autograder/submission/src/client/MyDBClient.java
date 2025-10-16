package autograder.submission.src.client;


import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class should implement your DB client.
 */
public class MyDBClient extends Client {
    private NodeConfig<String> nodeConfig= null;
    private final Map<String, Callback> pendingCallbacks = new ConcurrentHashMap<>();

    public MyDBClient() throws IOException {
    }
    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    /**
     * Override callbackSend to store the callback and attach a unique request ID.
     */
    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback callback) throws IOException {
        // Generate unique request ID
        String requestId = UUID.randomUUID().toString();

        // Store callback for this requestId
        pendingCallbacks.put(requestId, callback);

        // Prepend requestId to the request string (so server echoes it back)
        String requestWithId = requestId + "::" + request;

        // Send request
        super.send(isa, requestWithId);
    }

    /**
     * Override handleResponse to invoke the correct callback.
     */
    @Override
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        try {
            String response = new String(bytes, SingleServer.DEFAULT_ENCODING);

            // Extract requestId from response (assumes server sends it back as requestId::result)
            String[] parts = response.split("::", 2);
            if (parts.length == 2) {
                String requestId = parts[0];
                String result = parts[1];

                Callback cb = pendingCallbacks.remove(requestId);
                if (cb != null) {
                    cb.handleResponse(result.getBytes(SingleServer.DEFAULT_ENCODING), header);
                }
            } else {
                // fallback if server did not echo requestId
                System.out.println("Received response: " + response);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
    /**
     * Example main to test sending a query.
     */
    public static void main(String[] args) throws IOException {
        MyDBClient client = new MyDBClient();
        InetSocketAddress serverAddr = server.SingleServer.getSocketAddress(args);

        client.callbackSend(serverAddr, "SELECT * FROM users;", new Callback() {
            @Override
            public void handleResponse(byte[] bytes, NIOHeader header) {
                try {
                    System.out.println("Callback got response: " +
                            new String(bytes, SingleServer.DEFAULT_ENCODING));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
