package coprocessor;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;

// cc DelayRegionCloseObserver Special test observer creating delays
public class DelayRegionCloseObserver extends BaseRegionObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  // vv DelayRegionCloseObserver
  private Random rnd = new Random();

  @Override
  public StoreFile.Reader preStoreFileReaderOpen(
    ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p,
    FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r,
    StoreFile.Reader reader) throws IOException {
    try {
      long delay = rnd.nextInt(3);
      LOG.info("@@@ Delaying region " +
        ctx.getEnvironment().getRegion().getRegionInfo().
          getRegionNameAsString() + " for " + delay + " seconds...");
      Thread.sleep(delay * 1000);
    } catch (InterruptedException ie) {
      LOG.error(ie);
    }
    return reader;
  }
  // ^^ DelayRegionCloseObserver
}
