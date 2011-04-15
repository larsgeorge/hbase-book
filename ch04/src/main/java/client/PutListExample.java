package client;

// cc PutListExample Example inserting data into HBase using a list
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutListExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    HTable table = new HTable(conf, "testtable");

    // vv PutListExample
    List<Put> puts = new ArrayList<Put>(); // co PutListExample-1-CreateList Create a list that holds the Put instances.

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    puts.add(put1); // co PutListExample-2-AddPut1 Add put to list.

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    puts.add(put2); // co PutListExample-3-AddPut2 Add another put to list.

    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val3"));
    puts.add(put3); // co PutListExample-4-AddPut3 Add third put to list.

    table.put(puts); // co PutListExample-5-DoPut Store multiple rows with columns into HBase.
    // ^^ PutListExample
  }
}
