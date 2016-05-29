package coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

// cc RegionServerObserverExample Example region server observer that ...
public class RegionServerObserverExample extends BaseRegionServerObserver {
  public static final Log LOG = LogFactory.getLog(Region.class);

  @Override
  public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c,
    Region regionA, Region regionB, Region mergedRegion) throws IOException {
    HRegionInfo regionInfo = mergedRegion.getRegionInfo();
//    mergedRegion.getWAL().append(mergedRegion.getTableDesc(), regionInfo,
//      new WALKey(mergedRegion.getRegionName(), regionInfo.getTable()),
//      new WALEdit().add(CellUtil.createCell()))
  }

}
