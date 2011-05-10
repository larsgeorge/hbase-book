package client;

// cc PutListErrorExample1 Example inserting a faulty column family into HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutListErrorExample1 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    HTable table = new HTable(conf, "testtable");

    List<Put> puts = new ArrayList<Put>();

    // vv PutListErrorExample1
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    puts.add(put1);
    Put put2 = new Put(Bytes.toBytes("row2"));
    /*[*/put2.add(Bytes.toBytes("BOGUS"),/*]*/ Bytes.toBytes("qual1"),
      Bytes.toBytes("val2")); // co PutListErrorExample1-1-AddErrorPut Add put with non existent family to list.
    puts.add(put2);
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val3"));
    puts.add(put3);

    table.put(puts); // co PutListErrorExample1-2-DoPut Store multiple rows with columns into HBase.
    // ^^ PutListErrorExample1
  }
}
