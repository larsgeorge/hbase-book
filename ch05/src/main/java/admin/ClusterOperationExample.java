package admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc ClusterOperationExample Shows the use of the cluster operations
public class ClusterOperationExample {

  private static void printRegionInfo(List<HRegionInfo> infos) {
    for (HRegionInfo info : infos) {
      System.out.println("  Start Key: " + Bytes.toString(info.getStartKey()));
    }
  }

  private static List<HRegionInfo> filterTableRegions(List<HRegionInfo> regions,
    TableName tableName) {
    List<HRegionInfo> filtered = new ArrayList<>();
    for (HRegionInfo info : regions) {
      if (info.getTable().equals(tableName))
        filtered.add(info);
    }
    return filtered;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    // vv ClusterOperationExample
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    TableName tableName = TableName.valueOf("testtable");
    HColumnDescriptor coldef1 = new HColumnDescriptor("colfam1");
    HTableDescriptor desc = new HTableDescriptor(tableName)
      .addFamily(coldef1)
      .setValue("Description", "Chapter 5 - ClusterOperationExample");
    byte[][] regions = new byte[][] { Bytes.toBytes("ABC"),
      Bytes.toBytes("DEF"), Bytes.toBytes("GHI"), Bytes.toBytes("KLM"),
      Bytes.toBytes("OPQ"), Bytes.toBytes("TUV")
    };
    admin.createTable(desc, regions); // co ClusterOperationExample-01-Create Create a table with seven regions, and one column family.

    BufferedMutator mutator = connection.getBufferedMutator(tableName);
    for (int a = 'A'; a <= 'Z'; a++)
      for (int b = 'A'; b <= 'Z'; b++)
        for (int c = 'A'; c <= 'Z'; c++) {
          String row = Character.toString((char) a) +
            Character.toString((char) b) + Character.toString((char) c); // co ClusterOperationExample-02-Put Insert many rows starting from "AAA" to "ZZZ". These will be spread across the regions.
          Put put = new Put(Bytes.toBytes(row));
          put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"),
            Bytes.toBytes("val1"));
          System.out.println("Adding row: " + row);
          mutator.mutate(put);
        }
    mutator.close();

    List<HRegionInfo> list = admin.getTableRegions(tableName);
    int numRegions = list.size();
    HRegionInfo info = list.get(numRegions - 1);
    System.out.println("Number of regions: " + numRegions); // co ClusterOperationExample-03-List List details about the regions.
    System.out.println("Regions: ");
    printRegionInfo(list);

    System.out.println("Splitting region: " + info.getRegionNameAsString());
    admin.splitRegion(info.getRegionName()); // co ClusterOperationExample-04-Split Split the last region this table has, starting at row key "TUV". Adds a new region starting with key "WEI".
    do {
      list = admin.getTableRegions(tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
    } while (list.size() <= numRegions); // co ClusterOperationExample-05-Wait Loop and check until the operation has taken effect.
    numRegions = list.size();
    System.out.println();
    System.out.println("Number of regions: " + numRegions);
    System.out.println("Regions: ");
    printRegionInfo(list);

    System.out.println("Retrieving region with row ZZZ...");
    RegionLocator locator = connection.getRegionLocator(tableName);
    HRegionLocation location =
      locator.getRegionLocation(Bytes.toBytes("ZZZ")); // co ClusterOperationExample-06-Cache Retrieve region infos cached and refreshed to show the difference.
    System.out.println("Found cached region: " +
      location.getRegionInfo().getRegionNameAsString());
    location = locator.getRegionLocation(Bytes.toBytes("ZZZ"), true);
    System.out.println("Found refreshed region: " +
      location.getRegionInfo().getRegionNameAsString());

    List<HRegionInfo> online =
      admin.getOnlineRegions(location.getServerName());
    online = filterTableRegions(online, tableName);
    int numOnline = online.size();
    System.out.println("Number of online regions: " + numOnline);
    System.out.println("Online Regions: ");
    printRegionInfo(online);

    HRegionInfo offline = online.get(online.size() - 1);
    System.out.println("Offlining region: " + offline.getRegionNameAsString());
    admin.offline(offline.getRegionName()); // co ClusterOperationExample-07-Offline Offline a region and print the list of all regions.
    int revs = 0;
    do {
      online = admin.getOnlineRegions(location.getServerName());
      online = filterTableRegions(online, tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
      revs++;
    } while (online.size() <= numOnline && revs < 10);
    numOnline = online.size();
    System.out.println();
    System.out.println("Number of online regions: " + numOnline);
    System.out.println("Online Regions: ");
    printRegionInfo(online);

    HRegionInfo split = online.get(0); // co ClusterOperationExample-08-Wrong Attempt to split a region with a split key that does not fall into boundaries. Triggers log message.
    System.out.println("Splitting region with wrong key: " +
      split.getRegionNameAsString());
    admin.splitRegion(split.getRegionName(),
      Bytes.toBytes("ZZZ")); // triggers log message

    System.out.println("Assigning region: " + offline.getRegionNameAsString());
    admin.assign(offline.getRegionName()); // co ClusterOperationExample-09-Reassign Reassign the offlined region.
    revs = 0;
    do {
      online = admin.getOnlineRegions(location.getServerName());
      online = filterTableRegions(online, tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
      revs++;
    } while (online.size() == numOnline && revs < 10);
    numOnline = online.size();
    System.out.println();
    System.out.println("Number of online regions: " + numOnline);
    System.out.println("Online Regions: ");
    printRegionInfo(online);

    System.out.println("Merging regions...");
    HRegionInfo m1 = online.get(0);
    HRegionInfo m2 = online.get(1);
    System.out.println("Regions: " + m1 + " with " + m2);
    admin.mergeRegions(m1.getEncodedNameAsBytes(), // co ClusterOperationExample-10-Merge Merge the first two regions. Print out result of operation.
      m2.getEncodedNameAsBytes(), false);
    revs = 0;
    do {
      list = admin.getTableRegions(tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
      revs++;
    } while (list.size() >= numRegions && revs < 10);
    numRegions = list.size();
    System.out.println();
    System.out.println("Number of regions: " + numRegions);
    System.out.println("Regions: ");
    printRegionInfo(list);

    // ^^ ClusterOperationExample
    locator.close();
    admin.close();
    connection.close();
  }
}
