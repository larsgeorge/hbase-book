package client;

// cc ScanSlicingExample Example using offset and limit parameters for scans
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.Table;

import util.HBaseHelper;

public class ScanSlicingExample {

  private static Table table = null;

  // vv ScanSlicingExample
  private static void scan(int num, int caching, int batch, int offset,
    int maxResults, int maxResultSize, boolean dump) throws IOException {
    int count = 0;
    Scan scan = new Scan()
      .setCaching(caching)
      .setBatch(batch)
      .setRowOffsetPerColumnFamily(offset)
      .setMaxResultsPerColumnFamily(maxResults)
      .setMaxResultSize(maxResultSize)
      .setScanMetricsEnabled(true);
    ResultScanner scanner = table.getScanner(scan);
    System.out.println("Scan #" + num + " running...");
    for (Result result : scanner) {
      count++;
      if (dump) System.out.println("Result [" + count + "]:" + result);
    }
    scanner.close();
    ScanMetrics metrics = scan.getScanMetrics();
    System.out.println("Caching: " + caching + ", Batch: " + batch +
      ", Offset: " + offset + ", maxResults: " + maxResults +
      ", maxSize: " + maxResultSize + ", Results: " + count +
      ", RPCs: " + metrics.countOfRPCcalls);
  }

  public static void main(String[] args) throws IOException {
    // ^^ ScanSlicingExample
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 10, 10, 2, true, "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    table = connection.getTable(TableName.valueOf("testtable"));

    // vv ScanSlicingExample
    /*...*/
    scan(1, 11, 0, 0, 2, -1, true);
    scan(2, 11, 0, 4, 2, -1, true);
    scan(3, 5, 0, 0, 2, -1, false);
    scan(4, 11, 2, 0, 5, -1, true);
    scan(5, 11, -1, -1, -1, 1, false);
    scan(6, 11, -1, -1, -1, 10000, false);
    /*...*/
    // ^^ ScanSlicingExample
    table.close();
    connection.close();
    helper.close();
    // vv ScanSlicingExample
  }
  // ^^ ScanSlicingExample
}
