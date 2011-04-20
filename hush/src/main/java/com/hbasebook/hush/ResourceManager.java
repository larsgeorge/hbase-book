package com.hbasebook.hush;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * This class is implemented as a Singleton, i.e., it is shared across the
 * entire application. There are accessors for all shared services included. <br/>
 * Using the class should be facilitated invoking <u>only</u>
 * <code>getInstance()
 * </code> without any parameters. This will return the globally configured
 * instance.
 */
public class ResourceManager {
  private final Log LOG = LogFactory.getLog(ResourceManager.class);

  private static ResourceManager INSTANCE;
  private final Configuration conf;
  private final HTablePool pool;
  private final Counters counters;
  private final DomainManager domainManager;
  private final UserManager userManager;

  /**
   * Returns the shared instance of this singleton class.
   * 
   * @return The singleton instance.
   * @throws IOException
   *           When creating the remote HBase connection fails.
   */
  public synchronized static ResourceManager getInstance() throws IOException {
    assert (INSTANCE != null);
    return INSTANCE;
  }

  /**
   * Creates a new instance using the provided configuration upon first
   * invocation, otherwise it returns the already existing instance.
   * 
   * @param conf
   *          The HBase configuration to use.
   * @return The new or existing singleton instance.
   * @throws IOException
   *           When creating the remote HBase connection fails.
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
   * @param conf
   *          The HBase configuration to use.
   * @throws IOException
   *           When creating the remote HBase connection fails.
   */
  private ResourceManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.pool = new HTablePool(conf, 10);
    this.counters = new Counters();
    this.domainManager = new DomainManager(this);
    this.userManager = new UserManager(this);
  }

  /**
   * Delayed initialization of the instance. Should be called once to set up the
   * counters etc.
   * 
   * @throws IOException
   *           When setting up the resources in HBase fails.
   */
  void init() throws IOException {
    counters.init();
  }

  /**
   * Returns the internal <code>HTable</code> pool.
   * 
   * @return The shared table pool.
   */
  public HTablePool getTablePool() {
    return pool;
  }

  /**
   * Returns a single table from the shared table pool. More convenient to use
   * compared to <code>getTablePool()</code>.
   * 
   * @param tableName
   *          The name of the table to retrieve.
   * @return The table reference.
   * @throws IOException
   *           When talking to HBase fails.
   */
  public HTable getTable(byte[] tableName) throws IOException {
    return (HTable) pool.getTable(tableName);
  }

  /**
   * Returns the previously retrieved tanle to the shared pool. The caller must
   * take care of calling <code>flushTable()</code> if there are any pending
   * mutatioons.
   * 
   * @param table
   *          The table reference to return to the pool.
   */
  public void putTable(HTable table) {
    if (table != null) {
      pool.putTable(table);
    }
  }

  /**
   * Returns the currently used configuration.
   * 
   * @return The current configuration.
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Convenience method to retrieve a new short Id. The value is returned in the
   * proper format to be used as a row key in the HBase table. Each call
   * increments the counter by one.
   * 
   * @return The newly created short Id.
   * @throws Exception
   *           When communicating with HBase fails.
   */
  public byte[] getShortId() throws IOException {
    return getShortId(1L);
  }

  /**
   * Convenience method to retrieve a new short Id. The value is returned in the
   * proper format to be used as a row key in the HBase table. Each call
   * increments the counter by the given <code>incrBy</code>.
   * 
   * @return The newly created short Id.
   * @param incrBy
   *          The increment value.
   * @throws Exception
   *           When communicating with HBase fails.
   */
  public byte[] getShortId(long incrBy) throws IOException {
    return counters.getShortId(incrBy);
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
   * @return A domain manager instance.
   */
  public DomainManager getDomainManager() {
    return domainManager;
  }

  /**
   * @return A user manager instance.
   */
  public UserManager getUserManager() {
    return userManager;
  }

}
