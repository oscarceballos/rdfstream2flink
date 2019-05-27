/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.util;

import static com.sleepycat.je.rep.impl.RepParams.NODE_HOST_PORT;

import java.io.File;
import java.util.StringTokenizer;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.utilint.TestHook;

/**
 * <p> In JE 4.2 the internal format changed for databases with duplicates
 * configured. Duplicates are configured if 
 * <code>DatabaseConfig.setSortedDuplicates(true)</code> is called in the base
 * API, or a <code>MANY_TO_ONE</code> or <code>MANY_TO_MANY</code> relationship
 * is used in the DPL API. Before upgrading environments which have duplicates
 * databases to JE 4.2, this utility must first be run using JE 4.1.</p>
 *
 * <p> This utility is used for replicated environments. For standalone 
 * environments, please use {@link com.sleepycat.je.util.DbPreUpgrade_4_1} 
 * instead.</p>
 *
 * The command line for this utility:
 * <pre>
 * java com.sleepycat.je.rep.util.DbRepPreUpgrade_4_1
 *   -h &lt;dir&gt;                  # environment home directory
 *   -groupName &lt;group name&gt;   # replication group name
 *   -nodeName &lt;node name&gt;     # replicated node name
 *   -nodeHostPort &lt;host:port&gt; # host name or IP address and port
 *                                     number to user for this node 
 *   -helperHosts &lt;host:port&gt;  # identifier for one or more members
 *                                     of the replication group which can
 *                                     be contacted for group information,
 *                                     in this format:
 *                                     hostname[:port][,hostname[:port]]
 * </pre>
 * See {@link DbRepPreUpgrade_4_1#main} for a full description of the command
 * line arguments.
 */ 
public class DbRepPreUpgrade_4_1 {

    private File envHome;
    private String groupName;
    private String nodeName;
    private String nodeHostPort;
    private String helperHosts;

    private static final String usageString =
        "usage: java -cp je.jar " +
        "com.sleepycat.je.rep.util.DbRepPreUpgrade_4_1\n" + 
        " -h <dir>                   # environment home directory\n" +
        " -groupName <group name>    # replication group name\n" +
        " -nodeName <node name>      # replicated node name\n" +
        " -nodeHostPort <host:port>  # host name or IP address and port\n" +
        "                              number to use for this node\n" +
        " -helperHosts <host:port>   # identifier for one or more members\n" +
        "                              of the replication group which can\n" +
        "                              be contacted for group information,\n" +
        "                              in this format:\n" +
        "                              hostname[:port][,hostname[:port]]\n";

    /**
     * Usage:
     * <pre>
     * java -cp je.jar com.sleepycat.je.rep.util.DbRepPreUpgrade_4_1
     *   -h &lt;dir&gt;                  # environment home directory
     *   -groupName &lt;group name&gt;   # replication group name
     *   -nodeName &lt;node name&gt;     # replicated node name
     *   -nodeHostPort &lt;host:port&gt; # host name or IP address and port
     *                                     number to use for this node
     *   -helperHosts $lt;host:port&gt;  # identifier for one or more members
     *                                     of the replication group which can
     *                                     be contacted for group information,
     *                                     in this format:
     *                                     hostname[:port][,hostname[:port]]
     * </pre>
     */
    public static void main(String[] args) {
        DbRepPreUpgrade_4_1 upgrader = new DbRepPreUpgrade_4_1();
        upgrader.parseArgs(args);

        try {
            upgrader.preUpgrade();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void printUsage(String msg) {
        System.err.println(msg);
        System.err.println(usageString);
        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        int argc = 0;
        int nArgs = args.length;

        while (argc < nArgs) {
            String thisArg = args[argc++].trim();
            if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(args[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = args[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-nodeName")) {
                if (argc < nArgs) {
                    nodeName = args[argc++];
                } else {
                    printUsage("-nodeName requires an argument");
                }
            } else if (thisArg.equals("-nodeHostPort")) {
                if (argc < nArgs) {
                    nodeHostPort = args[argc++];
                    try {
                        NODE_HOST_PORT.validateValue(nodeHostPort);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                        printUsage("-nodeHostPort is illegal");
                    }
                } else {
                    printUsage("-nodeHostPort requires an argument");
                }
            } else if (thisArg.equals("-helperHosts")) {
                if (argc < nArgs) {
                    helperHosts = args[argc++];
                    StringTokenizer st = new StringTokenizer(helperHosts, ",");
                    while (st.hasMoreElements()) {
                        try {
                            NODE_HOST_PORT.validateValue(st.nextToken());
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                            printUsage("-helperHosts has an invalid " +
                                       "host:port pair in the argumen");
                        }
                    } 
                } else {
                        printUsage("-helperHosts requires an argument");
                }
            } else {
                printUsage("Invalid argument");
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }

        if (groupName == null) {
            printUsage("-groupName is a required argument");
        }

        if (nodeName == null) {
            printUsage("-nodeName is a required argument");
        }

        if (nodeHostPort == null) {
            printUsage("-nodeHostPort is a required argument");
        }

        if (helperHosts == null) {
            printUsage("-helperHosts is a required argument");
        }
    }

    private DbRepPreUpgrade_4_1() {
    }

    /**
     * Create a DbRepUpgrade object for a specified node.
     *
     * @param envHome The node's environment directory
     * @param groupName The name of the new replication group
     * @param nodeName The node's name
     * @param nodeHostPort The host and port for this node
     * @param helperHosts The helper nodes for this node to contact master
     */
    public DbRepPreUpgrade_4_1(File envHome, 
                               String groupName, 
                               String nodeName, 
                               String nodeHostPort,
                               String helperHosts) {
        this.envHome = envHome;
        this.groupName = groupName;
        this.nodeName = nodeName;
        this.nodeHostPort = nodeHostPort;
        this.helperHosts = helperHosts;
    }

     /**
      * Ensure sure that a ReplicatedEnvironment which has duplicates databases
      * can be upgraded to JE 4.2.
      */
    public void preUpgrade() {
        /* Open this ReplicatedEnvironment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        ReplicationConfig repConfig = 
            new ReplicationConfig(groupName, nodeName, nodeHostPort);
        repConfig.setHelperHosts(helperHosts);
        /* Don't join the group while doing upgrade, just do sync. */
        repConfig.setConfigParam
            (RepParams.DONT_JOIN_REP_GROUP.getName(), "true");
                                
        ReplicatedEnvironment repEnv = 
            new ReplicatedEnvironment(envHome, repConfig, envConfig);

        /* Do the sync. */
        repEnv.sync();

        /* Close this ReplicatedEnvironment. */
        repEnv.close();
    }
}
