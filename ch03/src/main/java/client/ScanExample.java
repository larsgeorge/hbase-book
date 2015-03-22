package client;

// cc ScanExample Example using a scanner to access data in a table
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class ScanExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    // Tip: Remove comment below to enable padding, adjust start and stop
    // row, as well as columns below to match. See scan #5 comments.
    helper.fillTable("testtable", 1, 100, 100, /* 3, false, */ "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    System.out.println("Scanning table #1...");
    // vv ScanExample
    Scan scan1 = new Scan(); // co ScanExample-1-NewScan Create empty Scan instance.
    ResultScanner scanner1 = table.getScanner(scan1); // co ScanExample-2-GetScanner Get a scanner to iterate over the rows.
    for (Result res : scanner1) {
      System.out.println(res); // co ScanExample-3-Dump Print row content.
    }
    scanner1.close(); // co ScanExample-4-Close Close scanner to free remote resources.

    // ^^ ScanExample
    System.out.println("Scanning table #2...");
    // vv ScanExample
    Scan scan2 = new Scan();
    scan2.addFamily(Bytes.toBytes("colfam1")); // co ScanExample-5-AddColFam Add one column family only, this will suppress the retrieval of "colfam2".
    ResultScanner scanner2 = table.getScanner(scan2);
    for (Result res : scanner2) {
      System.out.println(res);
    }
    scanner2.close();

    // ^^ ScanExample
    System.out.println("Scanning table #3...");
    // vv ScanExample
    Scan scan3 = new Scan();
    scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
      addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-33")). // co ScanExample-6-Build Use fluent pattern to add specific details to the Scan.
      setStartRow(Bytes.toBytes("row-10")).
      setStopRow(Bytes.toBytes("row-20"));
    ResultScanner scanner3 = table.getScanner(scan3);
    for (Result res : scanner3) {
      System.out.println(res);
    }
    scanner3.close();

    // ^^ ScanExample
    System.out.println("Scanning table #4...");
    // vv ScanExample
    Scan scan4 = new Scan();
    scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")). // co ScanExample-7-Build Only select one column.
      setStartRow(Bytes.toBytes("row-10")).
      setStopRow(Bytes.toBytes("row-20"));
    ResultScanner scanner4 = table.getScanner(scan4);
    for (Result res : scanner4) {
      System.out.println(res);
    }
    scanner4.close();

    // ^^ ScanExample
    System.out.println("Scanning table #5...");
    // vv ScanExample
    Scan scan5 = new Scan();
    // ^^ ScanExample
    // When using padding above, use "col-005", and "row-020", "row-010".
    // vv ScanExample
    scan5.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
      setStartRow(Bytes.toBytes("row-20")).
      setStopRow(Bytes.toBytes("row-10")).
      setReversed(true); // co ScanExample-8-Build One column scan that runs in reverse.
    ResultScanner scanner5 = table.getScanner(scan5);
    for (Result res : scanner5) {
      System.out.println(res);
    }
    scanner5.close();
    // ^^ ScanExample
    table.close();
    connection.close();
    helper.close();
  }
}
