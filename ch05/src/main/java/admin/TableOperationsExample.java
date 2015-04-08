package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc TableOperationsExample Example using the various calls to disable, enable, and check that status of a table
public class TableOperationsExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    // vv TableOperationsExample
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    TableName tableName = TableName.valueOf("testtable");
    HTableDescriptor desc = new HTableDescriptor(tableName);
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
      admin.deleteTable(tableName);
    } catch (IOException e) {
      System.err.println("Error deleting table: " + e.getMessage());
    }

    // ^^ TableOperationsExample
    System.out.println("Disabling table...");
    // vv TableOperationsExample
    admin.disableTable(tableName);
    boolean isDisabled = admin.isTableDisabled(tableName);
    System.out.println("Table is disabled: " + isDisabled);

    boolean avail1 = admin.isTableAvailable(tableName);
    System.out.println("Table available: " + avail1);

    // ^^ TableOperationsExample
    System.out.println("Deleting disabled table...");
    // vv TableOperationsExample
    admin.deleteTable(tableName);

    boolean avail2 = admin.isTableAvailable(tableName);
    System.out.println("Table available: " + avail2);

    // ^^ TableOperationsExample
    System.out.println("Creating table again...");
    // vv TableOperationsExample
    admin.createTable(desc);
    boolean isEnabled = admin.isTableEnabled(tableName);
    System.out.println("Table is enabled: " + isEnabled);
    // ^^ TableOperationsExample
  }
}
