package client;

// cc ScanTimeoutExample Example timeout while using a scanner
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import util.HBaseHelper;

public class ScanTimeoutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv ScanTimeoutExample
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);

    int scannerTimeout = (int) conf.getLong(
      HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1); // co ScanTimeoutExample-1-GetConf Get currently configured lease timeout.
    // ^^ ScanTimeoutExample
    System.out.println("Current (local) lease period: " + scannerTimeout + "ms");
    System.out.println("Sleeping now for " + (scannerTimeout + 5000) + "ms...");
    // vv ScanTimeoutExample
    try {
      Thread.sleep(scannerTimeout + 5000); // co ScanTimeoutExample-2-Sleep Sleep a little longer than the lease allows.
    } catch (InterruptedException e) {
      // ignore
    }
    // ^^ ScanTimeoutExample
    System.out.println("Attempting to iterate over scanner...");
    // vv ScanTimeoutExample
    while (true){
      try {
        Result result = scanner.next();
        if (result == null) break;
        System.out.println(result); // co ScanTimeoutExample-3-Dump Print row content.
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
    }
    scanner.close();
    // ^^ ScanTimeoutExample
    table.close();
    connection.close();
    helper.close();
  }
}
