package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc CreateTableWithNamespaceExample Example using the administrative API to create a table with a custom namespace
public class CreateTableWithNamespaceExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    // vv CreateTableWithNamespaceExample
    /*[*/NamespaceDescriptor namespace =
      NamespaceDescriptor.create("testspace").build();
    admin.createNamespace(namespace);/*]*/

    TableName tableName = TableName.valueOf("testspace", "testtable");
    HTableDescriptor desc = new HTableDescriptor(tableName);

    HColumnDescriptor coldef = new HColumnDescriptor(
      Bytes.toBytes("colfam1"));
    desc.addFamily(coldef);

    admin.createTable(desc);
    // ^^ CreateTableWithNamespaceExample

    boolean avail = admin.isTableAvailable(tableName);
    System.out.println("Table available: " + avail);
  }
}
