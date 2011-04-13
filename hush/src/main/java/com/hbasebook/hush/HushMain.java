package com.hbasebook.hush;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

import com.hbasebook.hush.schema.SchemaManager;
import com.hbasebook.hush.table.UserTable;

public class HushMain {

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("HushMain", options, true);
    System.exit(exitCode);
  }

  private static void createAdminUser(ResourceManager resourceManager)
      throws IOException {
    HTable table = resourceManager.getTable(UserTable.NAME);
    byte[] ADMIN_LOGIN = Bytes.toBytes("admin");
    byte[] ADMIN_PASSWORD = ADMIN_LOGIN;
    byte[] ADMIN_ROLES = Bytes.toBytes("admin,user");

    if (!table.exists(new Get(ADMIN_LOGIN))) {
      Put put = new Put(ADMIN_LOGIN);
      put.add(UserTable.DATA_FAMILY, UserTable.CREDENTIALS, ADMIN_PASSWORD);
      put.add(UserTable.DATA_FAMILY, UserTable.ROLES, ADMIN_ROLES);
      table.put(put);
      table.flushCommits();
    }
    resourceManager.putTable(table);
  }

  public static void main(String[] args) throws Exception {
    Log LOG = LogFactory.getLog(HushMain.class);

    // get HBase configuration and shared resource manager
    LOG.info("Initializing HBase");
    Configuration conf = HBaseConfiguration.create();
    ResourceManager manager = ResourceManager.getInstance(conf);

    LOG.info("Creating/updating HBase schema");
    // create or update the schema
    SchemaManager schemaManager = new SchemaManager(conf, "schema.xml");
    schemaManager.process();

    // set up command line options
    Options options = new Options();
    options.addOption("p", "port", true, "Port to bind to [default: 8080]");

    // parse command line parameters
    CommandLine commandLine = null;
    try {
      commandLine = new PosixParser().parse(options, args);
    } catch (ParseException e) {
      LOG.error("Could not parse command line args: ", e);
      printUsageAndExit(options, -1);
    }

    // user provided value precedes config value
    if (commandLine != null && commandLine.hasOption("port")) {
      String val = commandLine.getOptionValue("port");
      manager.getConfiguration().setInt("hush.port", Integer.parseInt(val));
      LOG.debug("Port set to: " + val);
    }

    // get port to bind to
    int port = manager.getConfiguration().getInt("hush.port", 8080);

    LOG.info("Web server setup.");

    // create server and configure basic settings
    Server server = new Server();
    server.setStopAtShutdown(true);

    // set up connector
    Connector connector = new SelectChannelConnector();
    connector.setPort(port);
    // connector.setHost("127.0.0.1");
    server.addConnector(connector);

    // set up context
    WebAppContext wac = new WebAppContext();
    wac.setContextPath("/");

    // expanded war or path of war file
    wac.setWar("./hush/src/main/webapp");
    server.setHandler(wac);

    // configure security
    LOG.info("Configuring security.");
    createAdminUser(manager);
    LoginService loginService = new HBaseLoginService("HBaseRealm");
    server.addBean(loginService);
    wac.getSecurityHandler().setLoginService(loginService);

    // start the server
    server.start();
    server.join();
  }
}
