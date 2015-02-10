package com.hbasebook.hush;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import com.hbasebook.hush.schema.SchemaManager;
import com.hbasebook.hush.servlet.security.HBaseLoginService;

/**
 * Main application class for Hush - The HBase URL Shortener.
 */
public class HushMain {

  /**
   * Helper method to print out the command line arguments available.
   *
   * @param options
   *          The command line argument definition.
   * @param exitCode
   *          The exit code to use when exiting the application.
   */
  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("HushMain", options, true);
    System.exit(exitCode);
  }

  /**
   * Main entry point to application. Sets up the resources and launches the
   * Jetty server.
   *
   * @param args
   *          The command line arguments.
   * @throws Exception
   *           When there is an issue launching the application.
   */
  public static void main(String[] args) throws Exception {
    Log LOG = LogFactory.getLog(HushMain.class);

    // get HBase configuration
    LOG.info("Initializing HBase");
    Configuration conf = HBaseConfiguration.create();
    // create or update the schema
    LOG.info("Creating/updating HBase schema");
    SchemaManager schemaManager = new SchemaManager(conf, "schema.xml");
    schemaManager.process();

    ResourceManager manager = ResourceManager.getInstance(conf);
    manager.init();

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
    int port = ResourceManager.getHushPort();

    LOG.info("Web server setup.");

    // create server and configure basic settings
    Server server = new Server(port);
    server.setStopAtShutdown(true);

    // set up context
    WebAppContext wac = new WebAppContext();
    wac.setContextPath("/");

    // expanded war or path of war file
    wac.setWar("src/main/webapp");
    server.setHandler(wac);

    // configure security
    LOG.info("Configuring security.");
    HBaseLoginService loginService = new HBaseLoginService("HBaseRealm");
    server.addBean(loginService);
    wac.getSecurityHandler().setLoginService(loginService);

    // start the server
    server.start();
    server.join();
  }
}
