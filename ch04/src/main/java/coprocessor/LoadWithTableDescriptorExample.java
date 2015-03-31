package coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import util.HBaseHelper;

// cc LoadWithTableDescriptorExample Load a coprocessor using the table descriptor
// vv LoadWithTableDescriptorExample
public class LoadWithTableDescriptorExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    // ^^ LoadWithTableDescriptorExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv LoadWithTableDescriptorExample
    TableName tableName = TableName.valueOf("testtable");

    HTableDescriptor htd = new HTableDescriptor(tableName); // co LoadWithTableDescriptorExample-1-Define Define a table descriptor.
    htd.addFamily(new HColumnDescriptor("colfam1"));
    htd.setValue("COPROCESSOR$1", "|" + // co LoadWithTableDescriptorExample-2-AddCP Add the coprocessor definition to the descriptor, while omitting the path to the JAR file.
      RegionObserverExample.class.getCanonicalName() +
      "|" + Coprocessor.PRIORITY_USER);

    Admin admin = connection.getAdmin(); // co LoadWithTableDescriptorExample-3-Admin Acquire an administrative API to the cluster and add the table.
    admin.createTable(htd);

    System.out.println(admin.getTableDescriptor(tableName)); // co LoadWithTableDescriptorExample-4-Check Verify if the definition has been applied as expected.
    admin.close();
    connection.close();
  }
}
// ^^ LoadWithTableDescriptorExample
