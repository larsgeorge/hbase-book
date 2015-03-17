package client;

// cc PutWriteBufferExample1 Example using the client-side write buffer
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PutWriteBufferExample1 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    // vv PutWriteBufferExample1
    TableName name = TableName.valueOf("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(name);
    BufferedMutator mutator = connection.getBufferedMutator(name); // co PutWriteBufferExample1-1-CheckFlush Get a mutator instance for the table.

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    mutator.mutate(put1); // co PutWriteBufferExample1-2-DoPut Store some rows with columns into HBase.

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    mutator.mutate(put2);

    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val3"));
    mutator.mutate(put3);

    Get get = new Get(Bytes.toBytes("row1"));
    Result res1 = table.get(get);
    System.out.println("Result: " + res1); // co PutWriteBufferExample1-3-Get1 Try to load previously stored row, this will print "Result: keyvalues=NONE".

    mutator.flush(); // co PutWriteBufferExample1-4-Flush Force a flush, this causes an RPC to occur.

    Result res2 = table.get(get);
    System.out.println("Result: " + res2); // co PutWriteBufferExample1-5-Get2 Now the row is persisted and can be loaded.

    mutator.close();
    table.close();
    connection.close();
    // ^^ PutWriteBufferExample1
    helper.close();
  }
}
