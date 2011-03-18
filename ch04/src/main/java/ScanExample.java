// cc ScanExample Example application inserting data into HBase
// vv ScanExample
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); // co ScanExample-1-CreateConf Create the configuration.

    // ^^ ScanExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 100, 100, "colfam1");
    // vv ScanExample
    HTable table = new HTable(conf, "testtable"); // co ScanExample-2-NewTable Instantiate a new client connection.

    Scan scan = new Scan(); // co ScanExample-3-NewScan Create put with specific row.
    scan.setCaching(3);
    scan.setBatch(3);
    ResultScanner scanner = table.getScanner(scan); // co ScanExample-5-DoScan Store row with column into HBase.
//    for (Result res : scanner) {
//      System.out.println(res);
//    }
    while (true) {
      Result[] res = scanner.next(10);
      if (res.length == 0) break;
      for (Result r : res) {
        System.out.println(r);
      }
    }
    scanner.close();
  }
}
// ^^ ScanExample
