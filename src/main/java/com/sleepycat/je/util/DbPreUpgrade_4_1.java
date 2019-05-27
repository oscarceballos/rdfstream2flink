/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.util;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.utilint.CmdUtil;

/**
 * <p> In JE 4.2 the internal format changed for databases with duplicates 
 * configured. Duplicates are configured if
 * <code>DatabaseConfig.setSortedDuplicates(true)</code> is called in the base
 * API, or a <code>MANY_TO_ONE</code> or <code>MANY_TO_MANY</code> relationship
 * is used in the DPL API. Before upgrading environments which have duplicates
 * databases to JE 4.2, this utility must first be run using JE 4.1.</p>
 *
 * <p> This utility is used for standalone environments. For replicated 
 * environments, please use 
 * {@link com.sleepycat.je.rep.util.DbRepPreUpgrade_4_1} instead.</p>
 *
 * The command line for this utility:
 * <pre>
 * java com.sleepycat.je.util.DbPreUpgrade_4_1
 *   -h &lt;dir&gt;        # environment home directory
 * </pre>
 * See {@link DbPreUpgrade_4_1#main} for a full description of the command line
 * arguments.
 */
public class DbPreUpgrade_4_1 {
    private File envHome;
    private Environment env;

    private static final String usageString =
        "usage: " + CmdUtil.getJavaCommand(DbPreUpgrade_4_1.class) + "\n" +
        "  -h <dir> # environment home directory\n";

    private DbPreUpgrade_4_1() {
    }

    /**
     * Create a DbPreUpgrade_4_1 object for a specific environment.
     *
     * @param envHome The home directory of the Environment that needs to be
     * upgraded to JE 4.2.
     */
    public DbPreUpgrade_4_1(File envHome) {
        this.envHome = envHome;
    }

    public static void main(String[] args)
        throws Exception {

        DbPreUpgrade_4_1 upgrader = new DbPreUpgrade_4_1();
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

        if (nArgs != 2) {
            printUsage("Invalid argument");
        }

        while (argc < nArgs) {
            String thisArg = args[argc++].trim();
            if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(args[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else {
                printUsage("Invalid argument");
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }
    }

    /**
     * Ensure that an Environment which has duplicates databases can be
     * upgraded to JE 4.2.
     */
    public void preUpgrade() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);

        Environment env = new Environment(envHome, envConfig);
        env.sync();
        env.close();
    }
}
