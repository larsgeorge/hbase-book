package client;

// cc BatchExample Example application using batch operations
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchExample {

    // vv BatchExample
    private final static byte[] ROW1 = Bytes.toBytes("row1");
    private final static byte[] ROW2 = Bytes.toBytes("row2");
    private final static byte[] COLFAM1 = Bytes.toBytes("colfam1"); // co BatchExample-1-Const Use constants for easy reuse.
    private final static byte[] COLFAM3 = Bytes.toBytes("colfam3");
    private final static byte[] QUAL1 = Bytes.toBytes("qual1");

    // ^^ BatchExample

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2", "colfam3");
    helper.put("testtable",
      new String[]{"row1"},
      new String[]{"colfam1", "colfam2"},
      new String[]{"qual1", "qual1", "qual2", "qual2", "qual3", "qual3"},
      new long[]{1, 2, 3, 4, 5, 6},
      new String[]{"val1", "val2", "val3", "val4", "val5", "val6"});
    System.out.println("Before batch call...");
    helper.dump("testtable", new String[]{ "row1", "row2" }, null, null);

    HTable table = new HTable(conf, "testtable");

    // vv BatchExample
    List<Row> batch = new ArrayList<Row>(); // co BatchExample-2-CreateList Create a list to hold all values.

    Put put = new Put(ROW2);
    put.add(COLFAM3, QUAL1, Bytes.toBytes("val7")); // co BatchExample-3-AddPut Add a Put instance.
    batch.add(put);

    Get get1 = new Get(ROW1);
    get1.addColumn(COLFAM1, QUAL1); // co BatchExample-4-AddGet Add a Get instance for a different row.
    batch.add(get1);

    Delete delete = new Delete(ROW1);
    delete.deleteColumns(COLFAM1, QUAL1); // co BatchExample-5-AddDelete Add a Delete instance.
    batch.add(delete);

    Get get2 = new Get(ROW2);
    get2.addFamily(Bytes.toBytes("BOGUS")); // co BatchExample-6-AddBogus Add a Get instance that will fail.
    batch.add(get2);

    Object[] results = new Object[batch.size()]; // co BatchExample-7-CreateResult Create result array.
    try {
      table.batch(batch, results);
    } catch (Exception e) {
      System.err.println("Error: " + e); // co BatchExample-8-Print Print error that was caught.
    }

    for (int i = 0; i < results.length; i++) {
      System.out.println("Result[" + i + "]: " + results[i]); // co BatchExample-9-Dump Print all results.
    }
    // ^^ BatchExample
    System.out.println("After batch call...");
    helper.dump("testtable", new String[]{ "row1", "row2" }, null, null);
  }
}
