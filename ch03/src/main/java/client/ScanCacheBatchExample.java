package client;

// cc ScanCacheBatchExample Example using caching and batch parameters for scans
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
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import util.HBaseHelper;

public class ScanCacheBatchExample {

  private static Table table = null;

  // vv ScanCacheBatchExample
  private static void scan(int caching, int batch) throws IOException {
    int count = 0;
    Scan scan = new Scan();
    scan.setCaching(caching);  // co ScanCacheBatchExample-1-Set Set caching and batch parameters.
    scan.setBatch(batch);
    scan.setScanMetricsEnabled(true);
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      count++; // co ScanCacheBatchExample-2-Count Count the number of Results available.
    }
    scanner.close();
    ScanMetrics metrics = scan.getScanMetrics();
    System.out.println("Caching: " + caching + ", Batch: " + batch +
      ", Results: " + count + ", RPCs: " + metrics.countOfRPCcalls);
  }

  public static void main(String[] args) throws IOException {
    // ^^ ScanCacheBatchExample
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    table = connection.getTable(TableName.valueOf("testtable"));

    // vv ScanCacheBatchExample
    /*...*/
    scan(1, 1);
    scan(200, 1);
    scan(2000, 100); // co ScanCacheBatchExample-3-Test Test various combinations.
    scan(2, 100);
    scan(2, 10);
    scan(5, 100);
    scan(5, 20);
    scan(10, 10);
    /*...*/
    // ^^ ScanCacheBatchExample
    table.close();
    connection.close();
    helper.close();
    // vv ScanCacheBatchExample
  }
  // ^^ ScanCacheBatchExample
}
