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

// cc ModifyTableExample Example modifying the structure of an existing table
public class ModifyTableExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv ModifyTableExample
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    TableName tableName = TableName.valueOf("testtable");
    
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor coldef1 = new HColumnDescriptor(
      Bytes.toBytes("colfam1"));
    desc.addFamily(coldef1);

    admin.createTable(desc); // co ModifyTableExample-1-CreateTable Create the table with the original structure.

    HTableDescriptor htd1 = admin.getTableDescriptor(tableName); // co ModifyTableExample-2-SchemaUpdate Get schema, update by adding a new family and changing the maximum file size property.
    HColumnDescriptor coldef2 = new HColumnDescriptor(
      Bytes.toBytes("colfam2"));
    htd1.addFamily(coldef2);
    htd1.setMaxFileSize(1024 * 1024 * 1024L);

    admin.disableTable(tableName);
    admin.modifyTable(tableName, htd1); // co ModifyTableExample-3-ChangeTable Disable, modify, and enable the table.
    admin.enableTable(tableName);

    HTableDescriptor htd2 = admin.getTableDescriptor(tableName);
    System.out.println("Equals: " + htd1.equals(htd2)); // co ModifyTableExample-4-Verify Check if the table schema matches the new one created locally.
    System.out.println("New schema: " + htd2);
    // ^^ ModifyTableExample
  }
}
