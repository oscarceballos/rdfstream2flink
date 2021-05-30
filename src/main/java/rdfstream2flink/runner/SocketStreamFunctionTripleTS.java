package rdfstream2flink.runner;

import org.apache.flink.util.IOUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SocketStreamFunctionTripleTS implements SourceFunction<TripleTS>, Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(SocketStreamFunctionTripleTS.class);

	/** Default delay between successive connection attempts */
	private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

	/** Default connection timeout when connecting to the server socket (infinite) */
	private static final int CONNECTION_TIMEOUT_TIME = 0;

	private final String hostname;
	private final int port;
	private final char delimiter;
	private final long maxNumRetries;
	private final long delayBetweenRetries;

	private transient Socket currentSocket;

	private volatile boolean isRunning = true;

	public SocketStreamFunctionTripleTS(String hostname, int port, char delimiter, long maxNumRetries) {
		this(hostname, port, delimiter, maxNumRetries, DEFAULT_CONNECTION_RETRY_SLEEP);
	}

	//65536
	public SocketStreamFunctionTripleTS(String hostname, int port, char delimiter, long maxNumRetries, long delayBetweenRetries) {
		checkArgument(port > 0 && port < 99999, "port is out of range");
		checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
		checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");

		this.hostname = checkNotNull(hostname, "hostname must not be null");
		this.port = port;
		this.delimiter = delimiter;
		this.maxNumRetries = maxNumRetries;
		this.delayBetweenRetries = delayBetweenRetries;
	}

	@Override
	public void run(SourceContext<TripleTS> ctx) {
		//while(isRunning){
		try(Socket socket = new Socket()){
			currentSocket = socket;
			currentSocket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
			RDFDataMgr.parse(new SourceContextAdapterTripleTS(ctx), socket.getInputStream(), Lang.NQ);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//}
	}

	@Override
	public void cancel() {
		isRunning = false;
		//we need to close the socket as well, because the Thread.interrupt() function will
		//not wake the thread in the socketStream.read() method when blocked.
		Socket theSocket = this.currentSocket;
		if(theSocket != null){
			IOUtils.closeSocket(theSocket);
		}
	}

	private static void checkProperty(Properties p, String key){
		if(!p.containsKey(key)){
			throw new IllegalArgumentException("Required property '" + key + "' not set");
		}
	}
}
