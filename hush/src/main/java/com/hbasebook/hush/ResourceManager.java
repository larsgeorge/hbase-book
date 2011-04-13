package com.hbasebook.hush;

import com.hbasebook.hush.table.HushTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ResourceManager {
  private final Log LOG = LogFactory.getLog(ResourceManager.class);

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
    if (INSTANCE != null) {
      INSTANCE = null;
    }
  }

  private ResourceManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.pool = new HTablePool(conf, 10);
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

  public static String encode(long number) {
    StringBuffer ret = new StringBuffer();
    while (number > 0) {
      if ((number % 36) < 10) {
        ret.append((char) (((int) '0') + (int) (number % 36)));
      } else {
        ret.append((char) (((int) 'A') + (int) ((number % 36) - 10)));
      }
      number /= 36;
    }
    return ret.toString();
  }

  public byte[] getShortId() throws Exception {
    HTableInterface table = pool.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, 1);

      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(
        HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID));
      return Bytes.toBytes(encode(id));
    } catch (Exception e) {
      LOG.error("Unable to a new short Id.", e);
      throw e;
    } finally {
      if (table != null) {
        try {
          pool.putTable(table);
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }
}
