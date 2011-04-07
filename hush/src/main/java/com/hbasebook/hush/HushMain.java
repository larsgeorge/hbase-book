package com.hbasebook.hush;

import com.hbasebook.hush.schema.SchemaManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

public class HushMain {

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("HushMain", options, true);
    System.exit(exitCode);
  }

  public static void main(String[] args) throws Exception {
    Log LOG = LogFactory.getLog(HushMain.class);

    // get HBase configuration and shared resource manager
    Configuration conf = HBaseConfiguration.create();
    ResourceManager manager = ResourceManager.getInstance(conf);

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
      manager.getConfiguration().setInt("hush.port",
        Integer.parseInt(val));
      LOG.debug("Port set to: " + val);
    }

    // get port to bind to
    int port = manager.getConfiguration().getInt("hush.port", 8080);

    // create server and configure basic settings
    Server server = new Server();
    server.setStopAtShutdown(true);

    // set up connector
    Connector connector = new SelectChannelConnector();
    connector.setPort(port);
    //connector.setHost("127.0.0.1");
    server.addConnector(connector);

    // set up context
    WebAppContext wac = new WebAppContext();
    wac.setContextPath("/");
    // expanded war or path of war file
    wac.setWar("./src/main/webapp");
    server.setHandler(wac);

    // start the server
    server.start();
    server.join();
  }
}
