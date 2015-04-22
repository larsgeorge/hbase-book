package coprocessor;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.AssignmentListener;
import org.apache.hadoop.hbase.regionserver.HRegion;

// cc DelayingMasterObserver Special master observer that delays region asignments
// vv DelayingMasterObserver
public class DelayingMasterObserver extends BaseMasterObserver
  implements AssignmentListener {
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  private Random rnd = new Random();

  @Override
  public void regionOpened(HRegionInfo hRegionInfo, ServerName serverName) {
    try {
      if (hRegionInfo.getTable().getQualifierAsString().equals("testtable")) {
        long delay = rnd.nextInt(3);
        LOG.info("@@@ Delaying region " +
          hRegionInfo.getRegionNameAsString() +
          " for " + delay + " seconds...");
        Thread.sleep(delay * 1000);
      }
    } catch (InterruptedException ie) {
      LOG.error(ie);
    }
  }

  @Override
  public void regionClosed(HRegionInfo hRegionInfo) {

  }

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    MasterCoprocessorEnvironment env = (MasterCoprocessorEnvironment) ctx;
    env.getMasterServices().getAssignmentManager().registerListener(this);
  }
}
// ^^ DelayingMasterObserver
