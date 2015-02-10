package com.hbasebook.hush;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * This class is implemented as a Singleton, i.e., it is shared across the
 * entire application. There are accessors for all shared services included.
 * <br/>
 * Using the class should be facilitated invoking <u>only</u>
 * <code>getInstance()</code> without any parameters. This will return the
 * globally configured instance.
 */
public class ResourceManager {
  private static final Log LOG = LogFactory.getLog(ResourceManager.class);

  private static final String HUSH_PORT = "hush.port";
  private static final int HUSH_PORT_DEFAULT = 8080;
  public static final byte[] ONE = new byte[] { 1 };
  public static final byte[] ZERO = new byte[] { 0 };

  private static ResourceManager INSTANCE;
  private final Configuration conf;
  private final Connection connection;
  private final Counters counters;
  private final DomainManager domainManager;
  private final UserManager userManager;
  private final UrlManager urlManager;
  private final LookupService lookupService;

  /**
   * Returns the shared instance of this singleton class.
   *
   * @return The singleton instance.
   * @throws IOException When creating the remote HBase connection fails.
   */
  public synchronized static ResourceManager getInstance() throws IOException {
    assert (INSTANCE != null);
    return INSTANCE;
  }

  /**
   * Creates a new instance using the provided configuration upon first
   * invocation, otherwise it returns the already existing instance.
   *
   * @param conf The HBase configuration to use.
   * @return The new or existing singleton instance.
   * @throws IOException When creating the remote HBase connection fails.
   */
  public synchronized static ResourceManager getInstance(Configuration conf)
      throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new ResourceManager(conf);
    }
    return INSTANCE;
  }

  /**
   * Stops the singleton instance and cleans up the internal reference.
   */
  public synchronized static void stop() {
    if (INSTANCE != null) {
      INSTANCE = null;
    }
  }

  /**
   * Internal constructor, called by the <code>getInstance()</code> methods.
   *
   * @param conf The HBase configuration to use.
   * @throws IOException When creating the remote HBase connection fails.
   */
  // cc HushHTablePoolProvider Use a pool of tables that is shared across
  // threads
  // vv HushHTablePoolProvider
  private ResourceManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.connection = ConnectionFactory.createConnection(conf);
    /* ... */
    // ^^ HushHTablePoolProvider
    this.counters = new Counters(this);
    this.domainManager = new DomainManager(this);
    this.userManager = new UserManager(this);
    this.urlManager = new UrlManager(this);
    URL url = getClass().getResource("/GeoIP.dat");
    File file = null;
    try {
      file = new File(url.toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    this.lookupService = file != null ? new LookupService(file) : null;
    // vv HushHTablePoolProvider
  }

  // ^^ HushHTablePoolProvider

  /**
   * Delayed initialization of the instance. Should be called once to set up the
   * counters etc.
   *
   * @throws IOException When setting up the resources in HBase fails.
   */
  void init() throws IOException {
    domainManager.init();
    urlManager.init();
    userManager.init();
  }

  /**
   * Returns a table from the shared connection instance.
   *
   * @param tableName The name of the table to retrieve.
   * @return The table reference.
   * @throws IOException When talking to HBase fails.
   */
  // vv HushHTablePoolProvider
  public Table getTable(TableName tableName) throws IOException {
    return connection.getTable(tableName);
  }

  // ^^ HushHTablePoolProvider

  /**
   * Discards the non-threadsafe instance of the previously retrieved table.
   * todo: Handle uncommitted mutations?
   *
   * @param table The table reference to return to the pool.
   */
  // vv HushHTablePoolProvider
  public void putTable(Table table) throws IOException {
    if (table != null) {
      table.close();
    }
  }

  // ^^ HushHTablePoolProvider

  /**
   * Discards the non-threadsafe instance of the previously retrieved table.
   * todo: Handle uncommitted mutations?
   *
   * @param table The table reference to return to the pool.
   */
  // vv HushHTablePoolProvider
  public void putTable(Table table, boolean quiet) throws IOException {
    if (table != null) {
      try {
        table.close();
      } catch(Throwable t) {
        if (!quiet) throw t;
      }
    }
  }

  // ^^ HushHTablePoolProvider

  /**
   * Returns the currently used configuration.
   *
   * @return The current configuration.
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Returns a reference to the shared counters instance.
   *
   * @return The shared counters instance.
   */
  public Counters getCounters() {
    return counters;
  }

  /**
   * Returns a reference to the shared domain manager instance.
   *
   * @return The shared domain manager instance.
   */
  public DomainManager getDomainManager() {
    return domainManager;
  }

  /**
   * Returns a reference to the shared user manager instance.
   *
   * @return The shared user manager instance.
   */
  public UserManager getUserManager() {
    return userManager;
  }

  /**
   * Returns a reference to the shared domain manager instance.
   *
   * @return The shared domain manager instance.
   */
  public UrlManager getUrlManager() {
    return urlManager;
  }

  /**
   * Returns the country details as looked up in the GeoIP database.
   *
   * @param address The IP address as given.
   * @return The country details.
   */
  public Country getCountry(String address) {
    return lookupService.getCountry(address);
  }

  public static int getHushPort() {
    try {
      return getInstance().getConfiguration().getInt(HUSH_PORT,
        HUSH_PORT_DEFAULT);
    } catch (IOException e) {
      LOG.error("Failed to get instance, defaulting port value.", e);
      return HUSH_PORT_DEFAULT;
    }
  }
}
