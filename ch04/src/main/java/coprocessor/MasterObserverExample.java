package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

// cc MasterObserverExample Example master observer that creates a separate directory on the file system when a table is created.
// vv MasterObserverExample
public class MasterObserverExample extends BaseMasterObserver {
  // ^^ MasterObserverExample
  public static final Log LOG = LogFactory.getLog(HRegion.class);
  // vv MasterObserverExample

  @Override
  public void postCreateTable(
    ObserverContext<MasterCoprocessorEnvironment> ctx,
    HTableDescriptor desc, HRegionInfo[] regions)
    throws IOException {
    // ^^ MasterObserverExample
    LOG.debug("Got postCreateTable callback");
    // vv MasterObserverExample
    TableName tableName = desc.getTableName(); // co MasterObserverExample-1-GetName Get the new table's name from the table descriptor.

    // ^^ MasterObserverExample
    LOG.debug("Created table: " + tableName + ", region count: " + regions.length);
    // vv MasterObserverExample
    MasterServices services = ctx.getEnvironment().getMasterServices();
    MasterFileSystem masterFileSystem = services.getMasterFileSystem(); // co MasterObserverExample-2-Services Get the available services and retrieve a reference to the actual file system.
    FileSystem fileSystem = masterFileSystem.getFileSystem();

    Path blobPath = new Path(tableName.getQualifierAsString() + "-blobs"); // co MasterObserverExample-3-Path Create a new directory that will store binary data from the client application.
    fileSystem.mkdirs(blobPath);

    // ^^ MasterObserverExample
    LOG.debug("Created " + blobPath + ": " + fileSystem.exists(blobPath));
    // vv MasterObserverExample
  }
}
// ^^ MasterObserverExample
