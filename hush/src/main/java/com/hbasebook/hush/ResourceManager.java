package com.hbasebook.hush;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

public class ResourceManager {
  private static ResourceManager INSTANCE;
  private final Configuration conf;
  private final HTablePool pool;

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
    if (INSTANCE != null) INSTANCE = null;
  }

  ResourceManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.pool = new HTablePool(conf, 10);
  }

  HTablePool getTablePool() {
    return pool;
  }

  Configuration getConfiguration() {
    return conf;
  }
}
