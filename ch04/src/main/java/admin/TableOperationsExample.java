package admin;

// cc TableOperationsExample Example using the various calls to disable, enable, and check that status of a table
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class TableOperationsExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    // vv TableOperationsExample
    HBaseAdmin admin = new HBaseAdmin(conf);

    HTableDescriptor desc = new HTableDescriptor(
      Bytes.toBytes("testtable"));
    HColumnDescriptor coldef = new HColumnDescriptor(
      Bytes.toBytes("colfam1"));
    desc.addFamily(coldef);
    // ^^ TableOperationsExample
    System.out.println("Creating table...");
    // vv TableOperationsExample
    admin.createTable(desc);

    // ^^ TableOperationsExample
    System.out.println("Deleting enabled table...");
    // vv TableOperationsExample
    try {
      admin.deleteTable(Bytes.toBytes("testtable"));
    } catch (IOException e) {
      System.err.println("Error deleting table: " + e.getMessage());
    }

    // ^^ TableOperationsExample
    System.out.println("Disabling table...");
    // vv TableOperationsExample
    admin.disableTable(Bytes.toBytes("testtable"));
    boolean isDisabled = admin.isTableDisabled(Bytes.toBytes("testtable"));
    System.out.println("Table is disabled: " + isDisabled);

    boolean avail1 = admin.isTableAvailable(Bytes.toBytes("testtable"));
    System.out.println("Table available: " + avail1);

    // ^^ TableOperationsExample
    System.out.println("Deleting disabled table...");
    // vv TableOperationsExample
    admin.deleteTable(Bytes.toBytes("testtable"));

    boolean avail2 = admin.isTableAvailable(Bytes.toBytes("testtable"));
    System.out.println("Table available: " + avail2);

    // ^^ TableOperationsExample
    System.out.println("Creating table again...");
    // vv TableOperationsExample
    admin.createTable(desc);
    boolean isEnabled = admin.isTableEnabled(Bytes.toBytes("testtable"));
    System.out.println("Table is enabled: " + isEnabled);
    // ^^ TableOperationsExample
  }
}
