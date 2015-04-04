package coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

// cc RegionObserverWithBypassExample Example region observer checking for special get requests and bypassing further processing
public class RegionObserverWithBypassExample extends BaseRegionObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);
  public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
    List<Cell> results) throws IOException {
    LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
    // vv RegionObserverWithBypassExample
    if (Bytes.equals(get.getRow(), FIXED_ROW)) {
      long time = System.currentTimeMillis();
      Cell cell = CellUtil.createCell(get.getRow(), FIXED_ROW, FIXED_ROW, // co RegionObserverWithBypassExample-1-Cell Create cell directly using the supplied utility.
        time, KeyValue.Type.Put.getCode(), Bytes.toBytes(time));
      // ^^ RegionObserverWithBypassExample
      LOG.debug("Had a match, adding fake cell: " + cell);
      // vv RegionObserverWithBypassExample
      results.add(cell);
      /*[*/e.bypass();/*]*/ // co RegionObserverWithBypassExample-2-Bypass Once the special cell is inserted all subsequent coprocessors are skipped.
    }
    // ^^ RegionObserverWithBypassExample
  }
}
