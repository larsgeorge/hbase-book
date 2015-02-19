package client;

// cc MissingRegionExample Example of how missing regions are handled
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseHelper;

import java.io.IOException;

public class MissingRegionExample {

  private static Connection connection = null;
  private static TableName tableName = null;

  private static void printTableRegions() throws IOException {
    System.out.println("Printing regions of table: " + tableName);
    Table table = connection.getTable(tableName);
    RegionLocator locator = connection.getRegionLocator(tableName);
    Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();
    for (int n = 0; n < pair.getFirst().length; n++) {
      byte[] sk = pair.getFirst()[n];
      byte[] ek = pair.getSecond()[n];
      System.out.println("[" + (n + 1) + "]" +
        " start key: " +
        (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
        ", end key: " +
        (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
    }
  }

    // vv MissingRegionExample
    static class Getter implements Runnable { // co MissingRegionExample-1-Thread Use asynchronous thread to continuously read from the table.
      @Override
      public void run() {
        try {
          while (true) {
            Table table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes("row-050"));
            long time = System.currentTimeMillis();
            table.get(get);
            long diff = System.currentTimeMillis() - time;
            if (diff > 1000) {
              System.out.println("Wait time: " + diff + "ms"); // co MissingRegionExample-2-Print Print out waiting time if the get call was taking longer than a second to complete.
            } else {
              System.out.print(".");
            }
            try {
              Thread.sleep(500); // co MissingRegionExample-3-Sleep1 Sleep for half a second.
            } catch (InterruptedException e) {
            }
          }
        } catch (IOException e) {
          System.err.println("Thread error: " + e);
        }
      }
    }

    // ^^ MissingRegionExample

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    tableName = TableName.valueOf("testtable");
    connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(tableName);

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    byte[][] regions = new byte[][] {
      Bytes.toBytes("row-030"),
      Bytes.toBytes("row-060")
    };
    helper.createTable("testtable", regions, "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 100, 1, 3, false, "colfam1", "colfam2");
    printTableRegions();

    // vv MissingRegionExample
    Admin admin = connection.getAdmin();

    Thread thread = new Thread(new Getter()); // co MissingRegionExample-4-Start Start the asynchronous thread.
    thread.setDaemon(true);
    thread.start();

    try {
      System.out.println("\nSleeping 3secs in main()..."); // co MissingRegionExample-5-Sleep2 Sleep for some time allow for the reading thread to print out some dots.
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // ignore
    }

    RegionLocator locator = connection.getRegionLocator(
      TableName.valueOf("testtable"));
    HRegionLocation location = locator.getRegionLocation(
      Bytes.toBytes("row-050"));
    System.out.println("\nUnassigning region: " + location.getRegionInfo().
      getRegionNameAsString());
    admin.closeRegion(location.getRegionInfo().getRegionName(), null); // co MissingRegionExample-6-Close Close the region containing the row the reading thread is retrieving. Note that unassign() does not work here because the master would automatically reopen the region when the thread is calling the get() method.

    int count = 0;
    while (locator.getAllRegionLocations().size() >= 3 && count++ < 10) // co MissingRegionExample-7-Check Use the number of online regions to confirm the close.
      try {
        System.out.println("\nWaiting for region to be offline in main()...");
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }

    try {
      System.out.println("\nSleeping 10secs in main()...");
      Thread.sleep(10000); // co MissingRegionExample-8-Sleep3 Sleep for another period of time to make the thread wait.
    } catch (InterruptedException e) {
      // ignore
    }

    System.out.println("\nAssigning region: " + location.getRegionInfo().
      getRegionNameAsString());
    admin.assign(location.getRegionInfo().getRegionName()); // co MissingRegionExample-9-Open Open the region, which will make the blocked get() in the thread wake up and print its waiting time.

    try {
      System.out.println("\nSleeping another 3secs in main()...");
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // ignore
    }
    locator.close();
    admin.close();
    // ^^ MissingRegionExample
    table.close();
    connection.close();
    helper.close();
  }
}
