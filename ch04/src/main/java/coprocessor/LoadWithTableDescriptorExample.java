package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.Coprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

// cc LoadWithTableDescriptorExample Example region observer checking for special get requests
// vv LoadWithTableDescriptorExample
public class LoadWithTableDescriptorExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // ^^ LoadWithTableDescriptorExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv LoadWithTableDescriptorExample

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(fs.getUri() + Path.SEPARATOR + "test.jar"); // co LoadWithTableDescriptorExample-1-Path Get the location of the JAR file containing the coprocessor implementation.

    HTableDescriptor htd = new HTableDescriptor("testtable"); // co LoadWithTableDescriptorExample-2-Define Define a table descriptor.
    htd.addFamily(new HColumnDescriptor("colfam1"));
    htd.setValue("COPROCESSOR$1", path.toString() +
      "|" + RegionObserverExample.class.getCanonicalName() + // co LoadWithTableDescriptorExample-3-AddCP Add the coprocessor definition to the descriptor.
      "|" + Coprocessor.Priority.USER);

    HBaseAdmin admin = new HBaseAdmin(conf); // co LoadWithTableDescriptorExample-4-Admin Instantiate an administrative API to the cluster and add the table.
    admin.createTable(htd);

    System.out.println(admin.getTableDescriptor(Bytes.toBytes("testtable"))); // co LoadWithTableDescriptorExample-5-Check Verify if the definition has been applied as expected.
  }
}
// ^^ LoadWithTableDescriptorExample
