package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc ServerAndRegionNameExample Shows the use of server and region names
public class ServerAndRegionNameExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    // vv ServerAndRegionNameExample
    TableName tableName = TableName.valueOf("testtable");
    HColumnDescriptor coldef1 = new HColumnDescriptor("colfam1");
    HTableDescriptor desc = new HTableDescriptor(tableName)
      .addFamily(coldef1)
      .setValue("Description", "Chapter 5 - ServerAndRegionNameExample");
    byte[][] regions = new byte[][] { Bytes.toBytes("ABC"),
      Bytes.toBytes("DEF"), Bytes.toBytes("GHI"), Bytes.toBytes("KLM"),
      Bytes.toBytes("OPQ"), Bytes.toBytes("TUV")
    };
    admin.createTable(desc, regions);

    RegionLocator locator = connection.getRegionLocator(tableName);
    HRegionLocation location = locator.getRegionLocation(Bytes.toBytes("Foo"));
    HRegionInfo info = location.getRegionInfo();
    System.out.println("Region Name: " + info.getRegionNameAsString());
    System.out.println("Server Name: " + location.getServerName());
    // ^^ ServerAndRegionNameExample
    locator.close();
    admin.close();
    connection.close();
  }
}
