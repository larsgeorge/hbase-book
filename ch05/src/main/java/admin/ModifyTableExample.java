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
import org.apache.hadoop.hbase.util.Pair;

import util.HBaseHelper;

// cc ModifyTableExample Example modifying the structure of an existing table
public class ModifyTableExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    Connection connection = ConnectionFactory.createConnection(conf);
    // vv ModifyTableExample
    Admin admin = connection.getAdmin();
    TableName tableName = TableName.valueOf("testtable");
    HColumnDescriptor coldef1 = new HColumnDescriptor("colfam1");
    HTableDescriptor desc = new HTableDescriptor(tableName)
      .addFamily(coldef1)
      .setValue("Description", "Chapter 5 - ModifyTableExample: Original Table");

    admin.createTable(desc, Bytes.toBytes(1L), Bytes.toBytes(10000L), 50); // co ModifyTableExample-1-CreateTable Create the table with the original structure and 50 regions.

    HTableDescriptor htd1 = admin.getTableDescriptor(tableName); // co ModifyTableExample-2-SchemaUpdate Get schema, update by adding a new family and changing the maximum file size property.
    HColumnDescriptor coldef2 = new HColumnDescriptor("colfam2");
    htd1
      .addFamily(coldef2)
      .setMaxFileSize(1024 * 1024 * 1024L)
      .setValue("Description",
        "Chapter 5 - ModifyTableExample: Modified Table");

    admin.disableTable(tableName);
    admin.modifyTable(tableName, htd1); // co ModifyTableExample-3-ChangeTable Disable and modify the table.

    Pair<Integer, Integer> status = new Pair<Integer, Integer>() {{ // co ModifyTableExample-4-Pair Create a status number pair to start the loop.
      setFirst(50);
      setSecond(50);
    }};
    for (int i = 0; status.getFirst() != 0 && i < 500; i++) {
      status = admin.getAlterStatus(desc.getTableName()); // co ModifyTableExample-5-Loop Loop over status until all regions are updated, or 500 seconds have been exceeded.
      if (status.getSecond() != 0) {
        int pending = status.getSecond() - status.getFirst();
        System.out.println(pending + " of " + status.getSecond()
          + " regions updated.");
        Thread.sleep(1 * 1000l);
      } else {
        System.out.println("All regions updated.");
        break;
      }
    }
    if (status.getFirst() != 0) {
      throw new IOException("Failed to update regions after 500 seconds.");
    }

    admin.enableTable(tableName);

    HTableDescriptor htd2 = admin.getTableDescriptor(tableName);
    System.out.println("Equals: " + htd1.equals(htd2)); // co ModifyTableExample-6-Verify Check if the table schema matches the new one created locally.
    System.out.println("New schema: " + htd2);
    // ^^ ModifyTableExample
  }
}
