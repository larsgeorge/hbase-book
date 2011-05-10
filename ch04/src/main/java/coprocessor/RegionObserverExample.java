package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

// cc RegionObserverExample Example region observer checking for special get requests
// vv RegionObserverExample
public class RegionObserverExample extends BaseRegionObserver {
  // ^^ RegionObserverExample
  public static final Log LOG = LogFactory.getLog(HRegion.class);
  // vv RegionObserverExample
  public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
    // ^^ RegionObserverExample
    LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
    // vv RegionObserverExample
    if (Bytes.equals(get.getRow(), FIXED_ROW)) { // co RegionObserverExample-1-Check Check if the request row key matches a well known one.
      KeyValue kv = new KeyValue(get.getRow(), FIXED_ROW, FIXED_ROW,
        Bytes.toBytes(System.currentTimeMillis()));
      // ^^ RegionObserverExample
      LOG.debug("Had a match, adding fake KV: " + kv);
      // vv RegionObserverExample
      results.add(kv); // co RegionObserverExample-2-Create Create a special KeyValue instance containing just the current time on the server.
    }
  }
}
// ^^ RegionObserverExample
