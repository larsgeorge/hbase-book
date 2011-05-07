package client;

// cc MissingRegionExample Example of how missing regions are handled
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseHelper;

import java.io.IOException;

public class MissingRegionExample {

  private static Configuration conf = HBaseConfiguration.create();

  private static void printTableRegions(String tableName) throws IOException { // co CreateTableWithRegionsExample-1-PrintTable Helper method to print the regions of a table.
    System.out.println("Printing regions of table: " + tableName);
    HTable table = new HTable(Bytes.toBytes(tableName));
    Pair<byte[][], byte[][]> pair = table.getStartEndKeys(); // co CreateTableWithRegionsExample-2-GetKeys Retrieve the start and end keys from the newly created table.
    for (int n = 0; n < pair.getFirst().length; n++) {
      byte[] sk = pair.getFirst()[n];
      byte[] ek = pair.getSecond()[n];
      System.out.println("[" + (n + 1) + "]" +
        " start key: " +
        (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) + // co CreateTableWithRegionsExample-3-Print Print the key, but guarding against the empty start (and end) key.
        ", end key: " +
        (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
    }
  }

    // vv MissingRegionExample
    static class Getter implements Runnable { // co MissingRegionExample-1-Thread Use asynchronous thread to update the same row but without a lock.
      @Override
      public void run() {
        try {
          while (true) {
            HTable table = new HTable(conf, "testtable");
            Get get = new Get(Bytes.toBytes("row-050"));
            long time = System.currentTimeMillis();
            table.get(get);
            long diff = System.currentTimeMillis() - time;
            if (diff > 1000) {
              System.out.println("Wait time: " + diff + "ms");
            } else {
              System.out.print(".");
            }
            try {
              Thread.sleep(500);
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
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    byte[][] regions = new byte[][] {
      Bytes.toBytes("row-030"),
      Bytes.toBytes("row-060")
    };
    helper.createTable("testtable", regions, "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 100, 1, 3, false, "colfam1", "colfam2");
    printTableRegions("testtable");

    HTable table = new HTable(conf, "testtable");

    // vv MissingRegionExample
    HBaseAdmin admin = new HBaseAdmin(conf);

    Thread thread = new Thread(new Getter()); // co MissingRegionExample-4-Start Start the asynchronous thread, which will block.
    thread.setDaemon(true);
    thread.start();

    try {
      System.out.println("\nSleeping 3secs in main()..."); // co MissingRegionExample-5-Sleep Sleep for some time to block other writers.
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // ignore
    }

    HRegionLocation location = table.getRegionLocation("row-050");
    System.out.println("\nUnassigning region: " + location.getRegionInfo().
      getRegionNameAsString());
    admin.closeRegion(location.getRegionInfo().getRegionName(), null);

    while (table.getRegionLocations().size() >= 3)
      try {
        System.out.println("\nWaiting for region to be offline in main()...");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

    try {
      System.out.println("\nSleeping 10secs in main()...");
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      // ignore
    }

    System.out.println("\nAssigning region: " + location.getRegionInfo().
      getRegionNameAsString());
    admin.assign(location.getRegionInfo().getRegionName(), false);

    try {
      System.out.println("\nSleeping another 3secs in main()...");
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // ignore
    }
    // ^^ MissingRegionExample
  }
}
