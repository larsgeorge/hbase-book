package admin;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

// cc DelayRegionCloseObserver Special test observer creating delays
public class DelayRegionCloseObserver extends BaseRegionObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  // vv DelayRegionCloseObserver
  private Random rnd = new Random();

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
    boolean abortRequested) throws IOException {
    try {
      long delay = rnd.nextInt(3);
      LOG.info("@@@ Delaying region " +
        c.getEnvironment().getRegion().getRegionNameAsString() +
        " for " + delay + " seconds...");
      Thread.sleep(delay * 1000);
    } catch (InterruptedException ie) {
      LOG.error(ie);
    }
  }
  // ^^ DelayRegionCloseObserver
}
