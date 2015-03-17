package client;

// cc GetMaxResultsRowOffsetExample1 Retrieves parts of a row with offset and limit
import java.io.IOException;

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

public class GetMaxResultsRowOffsetExample1 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 3, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv GetMaxResultsRowOffsetExample1
    Put put = new Put(Bytes.toBytes("row1"));
    for (int n = 1; n <= 1000; n++) {
      String num = String.format("%04d", n);
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual" + num),
        Bytes.toBytes("val" + num));
    }
    table.put(put);

    Get get1 = new Get(Bytes.toBytes("row1"));
    get1.setMaxResultsPerColumnFamily(10); // co GetMaxResultsRowOffsetExample1-1-Get1 Ask for ten cells to be returned at most.
    Result result1 = table.get(get1);
    CellScanner scanner1 = result1.cellScanner();
    while (scanner1.advance()) {
      System.out.println("Get 1 Cell: " + scanner1.current());
    }

    Get get2 = new Get(Bytes.toBytes("row1"));
    get2.setMaxResultsPerColumnFamily(10);
    get2.setRowOffsetPerColumnFamily(100); // co GetMaxResultsRowOffsetExample1-2-Get2 In addition, also skip the first 100 cells.
    Result result2 = table.get(get2);
    CellScanner scanner2 = result2.cellScanner();
    while (scanner2.advance()) {
      System.out.println("Get 2 Cell: " + scanner2.current());
    }

    // ^^ GetMaxResultsRowOffsetExample1
    table.close();
    connection.close();
    helper.close();
  }
}
