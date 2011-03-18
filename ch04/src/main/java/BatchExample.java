// cc BatchExample Example application using batch operations
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BatchExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HTable table = new HTable(conf, "testtable"); // co BatchExample-2-NewTable Instantiate a new client connection.

    // vv BatchExample
    Get get = new Get(Bytes.toBytes("row1")); // co BatchExample-3-NewGet Create get with specific row.

    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co BatchExample-4-AddCol Add a column to the get.

    Result result = table.get(get); // co BatchExample-5-DoGet Retrieve row with selected columns from HBase.

    byte[] val = result.getValue(Bytes.toBytes("colfam1"),
      Bytes.toBytes("qual1")); // co BatchExample-6-GetValue Get a specific value for the given column.

    System.out.println("Value: " + Bytes.toString(val)); // co BatchExample-7-Print Print out the value while converting it back.
    // ^^ BatchExample
  }
}
