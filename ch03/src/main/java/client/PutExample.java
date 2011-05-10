package client;

// cc PutExample Example application inserting data into HBase
// vv PutExample
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
// ^^ PutExample
import util.HBaseHelper;
// vv PutExample

import java.io.IOException;

public class PutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); // co PutExample-1-CreateConf Create the required configuration.

    // ^^ PutExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    // vv PutExample
    HTable table = new HTable(conf, "testtable"); // co PutExample-2-NewTable Instantiate a new client.

    Put put = new Put(Bytes.toBytes("row1")); // co PutExample-3-NewPut Create put with specific row.

    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1")); // co PutExample-4-AddCol1 Add a column, whose name is "colfam1:qual1", to the put.
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val2")); // co PutExample-4-AddCol2 Add another column, whose name is "colfam1:qual2", to the put.

    table.put(put); // co PutExample-5-DoPut Store row with column into the HBase table.
  }
}
// ^^ PutExample
