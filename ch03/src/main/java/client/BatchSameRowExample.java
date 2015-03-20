package client;

// cc BatchSameRowExample Example application using batch operations, modifying the same row
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class BatchSameRowExample {

  private final static byte[] ROW1 = Bytes.toBytes("row1");
  private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
  private final static byte[] QUAL1 = Bytes.toBytes("qual1");

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    helper.put("testtable", "row1", "colfam1", "qual1", 1L, "val1");
    System.out.println("Before batch call...");
    helper.dump("testtable", new String[] { "row1" }, null, null);

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv BatchSameRowExample
    List<Row> batch = new ArrayList<Row>();

    Put put = new Put(ROW1);
    put.addColumn(COLFAM1, QUAL1, 2L, Bytes.toBytes("val2"));
    batch.add(put);

    Get get1 = new Get(ROW1);
    get1.addColumn(COLFAM1, QUAL1);
    batch.add(get1);

    Delete delete = new Delete(ROW1);
    delete.addColumns(COLFAM1, QUAL1, 3L); // co BatchSameRowExample-1-AddDelete Delete the row that was just put above.
    batch.add(delete);

    Get get2 = new Get(ROW1);
    get1.addColumn(COLFAM1, QUAL1);
    batch.add(get2);

    Object[] results = new Object[batch.size()];
    try {
      table.batch(batch, results);
    } catch (Exception e) {
      System.err.println("Error: " + e);
    }
    // ^^ BatchSameRowExample

    for (int i = 0; i < results.length; i++) {
      System.out.println("Result[" + i + "]: type = " +
        results[i].getClass().getSimpleName() + "; " + results[i]);
    }
    table.close();
    connection.close();
    System.out.println("After batch call...");
    helper.dump("testtable", new String[]{"row1"}, null, null);
    helper.close();
  }
}
