package client;

// cc PutWriteBufferExample2 Example using the client-side write buffer
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PutWriteBufferExample2 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    TableName name = TableName.valueOf("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(name);
    BufferedMutator mutator = connection.getBufferedMutator(name);

    // vv PutWriteBufferExample2
    List<Mutation> mutations = new ArrayList<Mutation>(); // co PutWriteBufferExample2-1-DoPut Create a list to hold all mutations.

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    mutations.add(put1); // co PutWriteBufferExample2-2-DoPut Add Put instance to list of mutations.

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    mutations.add(put2);

    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val3"));
    mutations.add(put3);

    mutator.mutate(mutations); // co PutWriteBufferExample2-3-DoPut Store some rows with columns into HBase.

    Get get = new Get(Bytes.toBytes("row1"));
    Result res1 = table.get(get);
    System.out.println("Result: " + res1); // co PutWriteBufferExample2-4-Get1 Try to load previously stored row, this will print "Result: keyvalues=NONE".

    mutator.flush(); // co PutWriteBufferExample2-5-Flush Force a flush, this causes an RPC to occur.

    Result res2 = table.get(get);
    System.out.println("Result: " + res2); // co PutWriteBufferExample2-6-Get2 Now the row is persisted and can be loaded.
    // ^^ PutWriteBufferExample2
    mutator.close();
    table.close();
    connection.close();
    helper.close();
  }
}
