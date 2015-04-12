package admin;

import java.io.IOException;
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
      System.out.println("Start Key: " + Bytes.toString(info.getStartKey()));
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

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
    admin.createTable(desc, regions);

    BufferedMutator mutator = connection.getBufferedMutator(tableName);
    for (int a = 'A'; a <= 'Z'; a++)
      for (int b = 'A'; b <= 'Z'; b++)
        for (int c = 'A'; c <= 'Z'; c++) {
          String row = Character.toString((char) a) +
            Character.toString((char) b) + Character.toString((char) c);
          Put put = new Put(Bytes.toBytes(row));
          put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"),
            Bytes.toBytes("val1"));
          System.out.println("Adding row: " + row);
          mutator.mutate(put);
        }
    mutator.close();

    // vv ClusterOperationExample
    List<HRegionInfo> list = admin.getTableRegions(tableName);
    int numRegions = list.size();
    HRegionInfo info = list.get(numRegions - 1);
    System.out.println("Number of regions: " + numRegions);
    System.out.println("Regions: ");
    printRegionInfo(list);

    System.out.println("Splitting region: " + info.getRegionNameAsString());
    admin.splitRegion(info.getRegionName());
    do {
      list = admin.getTableRegions(tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
    } while (list.size() <= numRegions);
    numRegions = list.size();
    System.out.println();
    System.out.println("Number of regions: " + numRegions);
    System.out.println("Regions: ");
    printRegionInfo(list);

    Thread.sleep(30 * 1000);

    System.out.println("Retrieving region with row ZZZ...");
    RegionLocator locator = connection.getRegionLocator(tableName);
    HRegionLocation location =
      locator.getRegionLocation(Bytes.toBytes("ZZZ"));
    System.out.println("Found region: " +
      location.getRegionInfo().getRegionNameAsString());

    List<HRegionInfo> online =
      admin.getOnlineRegions(location.getServerName());
    int numOnline = online.size();
    System.out.println("Number of online regions: " + numOnline);
    System.out.println("Online Regions: " + online);
    printRegionInfo(online);

    HRegionInfo offline = online.get(1);
    System.out.println("Offlining region: " + offline.getRegionNameAsString());
    admin.offline(offline.getRegionName());
    do {
      online = admin.getOnlineRegions(location.getServerName());
      Thread.sleep(1 * 1000L);
      System.out.print(".");
    } while (online.size() <= numOnline);
    numOnline = online.size();
    System.out.println("Number of online regions: " + numOnline);
    System.out.println("Online Regions: ");
    printRegionInfo(online);

    HRegionInfo split = online.get(2);
    System.out.println("Splitting region: " +
      split.getRegionNameAsString());
    admin.splitRegion(split.getRegionName(), Bytes.toBytes("ZZZ"));

    System.out.println("Assigning region: " +
      offline.getRegionNameAsString());
    admin.assign(offline.getRegionName());
    do {
      online = admin.getOnlineRegions(location.getServerName());
      Thread.sleep(1 * 1000L);
      System.out.print(".");
    } while (online.size() == numOnline);
    numOnline = online.size();
    System.out.println("Number of online regions: " + numOnline);
    System.out.println("Online Regions: ");
    printRegionInfo(online);

    System.out.println("Merging regions...");
    admin.mergeRegions(online.get(numOnline - 2).getRegionName(),
      online.get(numOnline - 1).getRegionName(), false);
    do {
      list = admin.getTableRegions(tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
    } while (list.size() >= numRegions);
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
