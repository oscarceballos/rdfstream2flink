/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rdfstream2flink.runner;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.jena.graph.Triple;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 *
 * <p>
 * Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
@SuppressWarnings("serial")
public class LoadRDFStream {

    /**
     * Creates a new data stream that contains the triples received infinitely from a socket. Received strings are
     * decoded by the system's default character set, using "\n" as delimiter. The reader is terminated immediately when
     * the socket is down.
     *
     * @param environment
     * 		The environment which is the context
     * @param hostname
     * 		The host name which a server socket binds
     * @param port
     * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
     * 		allocated.
     * @return A data stream containing the triples received from the socket
     */

    /******** Triple ********/
    @PublicEvolving
    public static DataStreamSource<Triple> fromSocketTriple(StreamExecutionEnvironment environment, String hostname, int port, char delimiter, long maxRetry) {
        return environment.addSource(new SocketStreamFunctionTriple(hostname, port, delimiter, maxRetry), "Socket");
    }

    @PublicEvolving
    public static DataStreamSource<Triple> fromSocketTriple(StreamExecutionEnvironment environment, String hostname, int port, char delimiter) {
        return fromSocketTriple(environment, hostname, port, delimiter, 0);
    }

    @PublicEvolving
    public static DataStreamSource<Triple> fromSocketTriple(StreamExecutionEnvironment environment, String hostname, int port) {
        return fromSocketTriple(environment, hostname, port, '\n');
    }


    /******** TripleTS ********/
    @PublicEvolving
    public static DataStreamSource<TripleTS> fromSocketTripleTS(StreamExecutionEnvironment environment, String hostname, int port, char delimiter, long maxRetry) {
        return environment.addSource(new SocketStreamFunctionTripleTS(hostname, port, delimiter, maxRetry), "Socket");
    }

    @PublicEvolving
    public static DataStreamSource<TripleTS> fromSocketTripleTS(StreamExecutionEnvironment environment, String hostname, int port, char delimiter) {
        return fromSocketTripleTS(environment, hostname, port, delimiter, 0);
    }

    @PublicEvolving
    public static DataStreamSource<TripleTS> fromSocketTripleTS(StreamExecutionEnvironment environment, String hostname, int port) {
        return fromSocketTripleTS(environment, hostname, port, '\n');
    }
}
