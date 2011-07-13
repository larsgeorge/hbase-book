package client;

// cc PutWriteBufferExample Example using the client-side write buffer
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class PutWriteBufferExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    // vv PutWriteBufferExample
    HTable table = new HTable(conf, "testtable");
    System.out.println("Auto flush: " + table.isAutoFlush());  // co PutWriteBufferExample-1-CheckFlush Check what the auto flush flag is set to, should print "Auto flush: true".

    table.setAutoFlush(false); // co PutWriteBufferExample-2-SetFlush Set the auto flush to false to enable the client-side write buffer.

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    table.put(put1); // co PutWriteBufferExample-3-DoPut Store some rows with columns into HBase.

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    table.put(put2);

    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val3"));
    table.put(put3);

    Get get = new Get(Bytes.toBytes("row1"));
    Result res1 = table.get(get);
    System.out.println("Result: " + res1); // co PutWriteBufferExample-6-Get1 Try to load previously stored row, this will print "Result: keyvalues=NONE".

    table.flushCommits(); // co PutWriteBufferExample-7-Flush Force a flush, this causes an RPC to occur.

    Result res2 = table.get(get);
    System.out.println("Result: " + res2); // co PutWriteBufferExample-8-Get2 Now the row is persisted and can be loaded.
    // ^^ PutWriteBufferExample
  }
}
