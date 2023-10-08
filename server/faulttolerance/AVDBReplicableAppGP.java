package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class should implement your {@link Replicable} database app.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) This class must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class AVDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 200;

	// Important: This name must match that used in Grader
	public static final String TABLE_NAME = "grade";
	final private Session session;
	final private Cluster cluster;
	final private String myID;

	private int numRequests = 0;

	final public static Logger log = Logger.getLogger(AVDBReplicableAppGP
			.class.getSimpleName());

	/**
	 * @param args Singleton array specifying keyspace
	 * @throws IOException
	 */
	public AVDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		session = (cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(args[0]);
		this.myID = args[0];
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		return this.execute(request);
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO: get all safety-critical state, i.e., all state in
		// DEFAULT_TABLE_NAME, and return as a String
		String getAllRows = "select * from " + TABLE_NAME;
		ResultSet results = session.execute(getAllRows);
		String state = "";
		for (Row row : results) {
			state += row.toString();
		}

		System.out.println(this + " returning state: " + state);
		return state;
	}

	public String toString() {
		return this.getClass().getSimpleName() + ":" + this.myID;
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param serviceName
	 * @param state
	 * @return
	 */
	@Override
	public boolean restore(String serviceName, String state) {
		// TODO: Use the state arg to bootstrap your state
		numRequests = 0;
		return true;
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		//System.out.println("Received request: " + request);
		if (request instanceof RequestPacket) {
			String requestValue = ((RequestPacket) request).requestValue;

			//System.out.println(this + " executing request: " + request);
			log.log(Level.INFO, "{0} executing request #{1}:{2}", new
					Object[]{this, numRequests, request});
			ResultSet results = session.execute(requestValue);
			String response = "";
			for (Row row : results) {
				response += row.toString();
			}

			((RequestPacket) request).setResponse(response);
		}
		else {
			System.err.println("Unknown request type");
		}

		numRequests++;
		return true;
	}

	/**
	 * Convert serialized String to {@link gigapaxos.DBAppRequest}. Unnecessary
	 * if {@link RequestPacket} is being used, which almost certainly suffices
	 * for our purposes.
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		// TODO:
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. It is enough to use
	 * just a single type for all requests in this application as all requests
	 * are simply strings to be relayed to the backend data store.
	 * <p>
	 * Request types are unnecessary if using {@link RequestPacket} to
	 * encapsulate requests, which is enough for this application.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}

}