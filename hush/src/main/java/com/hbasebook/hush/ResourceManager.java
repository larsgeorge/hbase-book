package com.hbasebook.hush;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

public class ResourceManager {
  private final Log LOG = LogFactory.getLog(ResourceManager.class);

  private static ResourceManager INSTANCE;
  private final Configuration conf;
  private final HTablePool pool;
  private final Counters counters;

  public synchronized static ResourceManager getInstance() throws IOException {
    assert (INSTANCE != null);
    return INSTANCE;
  }

  public synchronized static ResourceManager getInstance(Configuration conf)
    throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new ResourceManager(conf);
    }
    return INSTANCE;
  }

  public synchronized static void stop() {
    if (INSTANCE != null) {
      INSTANCE = null;
    }
  }

  private ResourceManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.pool = new HTablePool(conf, 10);
    this.counters = new Counters();
  }

  void init() throws IOException {
    getShortId(Long.parseLong("1336", 36));
  }

  public HTablePool getTablePool() {
    return pool;
  }

  public HTable getTable(byte[] tableName) throws IOException {
    return (HTable) pool.getTable(tableName);
  }

  public void putTable(HTable table) throws IOException {
    if (table != null) {
      pool.putTable(table);
    }
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public byte[] getShortId() throws Exception {
    return getShortId(1L);
  }

  public byte[] getShortId(long incrBy) throws IOException {
    return counters.getShortId(incrBy);
  }

  public Counters getCounters() {
    return counters;
  }
}
