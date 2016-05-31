package client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc DeleteTimestampExample Example application deleting with explicit timestamps
public class DeleteTimestampExample {

  private final static byte[] ROW1 = Bytes.toBytes("row1");
  private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
  private final static byte[] QUAL1 = Bytes.toBytes("qual1");

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 3, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);

    TableName tableName = TableName.valueOf("testtable");
    Table table = connection.getTable(tableName);
    Admin admin = connection.getAdmin();

    // vv DeleteTimestampExample
    for (int count = 1; count <= 6; count++) { // co DeleteTimestampExample-1-Put Store the same column six times.
      Put put = new Put(ROW1);
      put.addColumn(COLFAM1, QUAL1, count, Bytes.toBytes("val-" + count)); // co DeleteTimestampExample-2-Add The version is set to a specific value, using the loop variable.
      table.put(put);
    }
    // ^^ DeleteTimestampExample
    System.out.println("After put calls...");
    helper.dump("testtable", new String[] { "row1" }, null, null);
//    admin.flush(tableName);
//    Thread.sleep(3000);
//    admin.majorCompact(tableName);
//    Thread.sleep(3000);
    // vv DeleteTimestampExample

    Delete delete = new Delete(ROW1); // co DeleteTimestampExample-3-Delete Delete the newest two versions.
    delete.addColumn(COLFAM1, QUAL1, 5);
    delete.addColumn(COLFAM1, QUAL1, 6);
    table.delete(delete);
    // ^^ DeleteTimestampExample

    System.out.println("After delete call...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);
  }
}
