package client;

// cc GetClosestRowBeforeExample Retrieves a row close to the requested, if necessary
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class GetClosestRowBeforeExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    List<Put> puts = new ArrayList<Put>();
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    puts.add(put1);
    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    puts.add(put2);
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val3"));
    puts.add(put3);
    table.put(puts);

    // vv GetClosestRowBeforeExample
    Get get1 = new Get(Bytes.toBytes("row3")); // co GetClosestRowBeforeExample-1-Get1 Attempt to read a row that does not exist.
    get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    Result result1 = table.get(get1);

    System.out.println("Get 1 isEmpty: " + result1.isEmpty());
    CellScanner scanner1 = result1.cellScanner();
    while (scanner1.advance()) {
      System.out.println("Get 1 Cell: " + scanner1.current());
    }

    Get get2 = new Get(Bytes.toBytes("row3"));
    get2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    get2.setClosestRowBefore(true); // co GetClosestRowBeforeExample-2-Get2 Instruct the get() call to fall back to the previous row, if necessary.
    Result result2 = table.get(get2);

    System.out.println("Get 2 isEmpty: " + result2.isEmpty());
    CellScanner scanner2 = result2.cellScanner();
    while (scanner2.advance()) {
      System.out.println("Get 2 Cell: " + scanner2.current());
    }

    Get get3 = new Get(Bytes.toBytes("row2")); // co GetClosestRowBeforeExample-3-Get3 Attempt to read a row that exists.
    get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    get3.setClosestRowBefore(true);
    Result result3 = table.get(get3);

    System.out.println("Get 3 isEmpty: " + result3.isEmpty());
    CellScanner scanner3 = result3.cellScanner();
    while (scanner3.advance()) {
      System.out.println("Get 3 Cell: " + scanner3.current());
    }

    Get get4 = new Get(Bytes.toBytes("row2")); // co GetClosestRowBeforeExample-4-Get4 Read exactly a row that exists.
    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    Result result4 = table.get(get4);

    System.out.println("Get 4 isEmpty: " + result4.isEmpty());
    CellScanner scanner4 = result4.cellScanner();
    while (scanner4.advance()) {
      System.out.println("Get 4 Cell: " + scanner4.current());
    }

    // ^^ GetClosestRowBeforeExample
    table.close();
    connection.close();
    helper.close();
  }
}
