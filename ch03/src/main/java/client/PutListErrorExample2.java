package client;

// cc PutListErrorExample2 Example inserting an empty Put instance into HBase
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PutListErrorExample2 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    List<Put> puts = new ArrayList<Put>();

    // vv PutListErrorExample2
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    puts.add(put1);
    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    puts.add(put2);
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val3"));
    puts.add(put3);
    /*[*/Put put4 = new Put(Bytes.toBytes("row2"));
    puts.add(put4);/*]*/ // co PutListErrorExample2-1-AddErrorPut Add put with no content at all to list.

    /*[*/try {/*]*/
      table.put(puts);
    /*[*/} catch (Exception e) {
      System.err.println("Error: " + e);
      // table.flushCommits();
      // todo: FIX!
      /*]*/ // co PutListErrorExample2-2-Catch Catch local exception and commit queued updates.
    /*[*/}/*]*/
    // ^^ PutListErrorExample2
    table.close();
    connection.close();
    helper.close();
  }
}
