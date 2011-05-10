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

// cc RegionObserverWithBypassExample Example region observer checking for special get requests and bypassing further processing
public class RegionObserverWithBypassExample extends BaseRegionObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);
  public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
    LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
    // vv RegionObserverWithBypassExample
    if (Bytes.equals(get.getRow(), FIXED_ROW)) {
      KeyValue kv = new KeyValue(get.getRow(), FIXED_ROW, FIXED_ROW,
        Bytes.toBytes(System.currentTimeMillis()));
      // ^^ RegionObserverWithBypassExample
      LOG.debug("Had a match, adding fake KV: " + kv);
      // vv RegionObserverWithBypassExample
      results.add(kv);
      /*[*/e.bypass();/*]*/ // co RegionObserverWithBypassExample-1-Bypass Once the special KeyValue is inserted all further processing is skipped.
    }
    // ^^ RegionObserverWithBypassExample
  }
}
