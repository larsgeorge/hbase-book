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

// cc LoadWithTableDescriptorExample2 Load a coprocessor using the table descriptor using provided method
public class LoadWithTableDescriptorExample2 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    TableName tableName = TableName.valueOf("testtable");

    // vv LoadWithTableDescriptorExample2
    HTableDescriptor htd = new HTableDescriptor(tableName); // co LoadWithTableDescriptorExample2-1-Create Use fluent interface to create and configure the instance.
    htd.addFamily(new HColumnDescriptor("colfam1"));
    /*[*/htd.addCoprocessor(RegionObserverExample.class.getCanonicalName(),
      null, Coprocessor.PRIORITY_USER, null);/*]*/ // co LoadWithTableDescriptorExample2-2-AddCP Use the provided method to add the coprocessor.

    Admin admin = connection.getAdmin();
    admin.createTable(htd);
    // ^^ LoadWithTableDescriptorExample2

    System.out.println(admin.getTableDescriptor(tableName));
    admin.close();
    connection.close();
  }
}
