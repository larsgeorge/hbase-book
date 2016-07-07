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
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import util.HBaseHelper;

// cc CreateTableWithRegionsExample Example using the administrative API to create a table with predefined regions
public class CreateTableWithRegionsExample {

  // vv CreateTableWithRegionsExample
  private static Configuration conf = null;
  private static Connection connection = null;

  private static void printTableRegions(String tableName) throws IOException { // co CreateTableWithRegionsExample-1-PrintTable Helper method to print the regions of a table.
    System.out.println("Printing regions of table: " + tableName);
    TableName tn = TableName.valueOf(tableName);
    RegionLocator locator = connection.getRegionLocator(tn);
    Pair<byte[][], byte[][]> pair = locator.getStartEndKeys(); // co CreateTableWithRegionsExample-2-GetKeys Retrieve the start and end keys from the newly created table.
    for (int n = 0; n < pair.getFirst().length; n++) {
      byte[] sk = pair.getFirst()[n];
      byte[] ek = pair.getSecond()[n];
      System.out.println("[" + (n + 1) + "]" +
        " start key: " +
        (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) + // co CreateTableWithRegionsExample-3-Print Print the key, but guarding against the empty start (and end) key.
        ", end key: " +
        (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
    }
    locator.close();
  }
  // ^^ CreateTableWithRegionsExample

  // vv CreateTableWithRegionsExample
  public static void main(String[] args) throws IOException, InterruptedException {
    conf = HBaseConfiguration.create();
    connection = ConnectionFactory.createConnection(conf);
    // ^^ CreateTableWithRegionsExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable1");
    helper.dropTable("testtable2");
    // vv CreateTableWithRegionsExample
    Admin admin = connection.getAdmin();

    HTableDescriptor desc = new HTableDescriptor(
      TableName.valueOf("testtable1"));
    HColumnDescriptor coldef = new HColumnDescriptor(
      Bytes.toBytes("colfam1"));
    desc.addFamily(coldef);

    admin.createTable(desc/*[*/, Bytes.toBytes(1L), Bytes.toBytes(100L), 10/*]*/); // co CreateTableWithRegionsExample-4-CreateTable1 Call the createTable() method while also specifying the region boundaries.
    printTableRegions("testtable1");

    byte[][] regions = new byte[][] { // co CreateTableWithRegionsExample-5-Regions Manually create region split keys.
      Bytes.toBytes("A"),
      Bytes.toBytes("D"),
      Bytes.toBytes("G"),
      Bytes.toBytes("K"),
      Bytes.toBytes("O"),
      Bytes.toBytes("T")
    };
    HTableDescriptor desc2 = new HTableDescriptor(
      TableName.valueOf("testtable2"));
    desc2.addFamily(coldef);
    admin.createTable(desc2, regions); // co CreateTableWithRegionsExample-6-CreateTable2 Call the createTable() method again, with a new table name and the list of region split keys.
    printTableRegions("testtable2");
  }
  // ^^ CreateTableWithRegionsExample
}
