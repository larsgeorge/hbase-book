package client;

// cc ScanCacheBatchExample Example using caching and batch parameters for scans
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import util.HBaseHelper;

import java.io.IOException;

public class ScanCacheBatchExample {

  private static HTable table = null;

  // vv ScanCacheBatchExample
  private static void scan(int caching, int batch) throws IOException {
    Logger log = Logger.getLogger("org.apache.hadoop");
    final int[] counters = {0, 0};
    Appender appender = new AppenderSkeleton() {
      @Override
      protected void append(LoggingEvent event) {
        String msg = event.getMessage().toString();
        if (msg != null && msg.contains("Call: next")) {
          counters[0]++;
        }
      }
      @Override
      public void close() {}
      @Override
      public boolean requiresLayout() {
        return false;
      }
    };
    log.removeAllAppenders();
    log.setAdditivity(false);
    log.addAppender(appender);
    log.setLevel(Level.DEBUG);


    Scan scan = new Scan();
    scan.setCaching(caching);  // co ScanCacheBatchExample-1-Set Set caching and batch parameters.
    scan.setBatch(batch);
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      counters[1]++; // co ScanCacheBatchExample-2-Count Count the number of Results available.
    }
    scanner.close();
    System.out.println("Caching: " + caching + ", Batch: " + batch +
      ", Results: " + counters[1] + ", RPCs: " + counters[0]);
  }

  public static void main(String[] args) throws IOException {
    // ^^ ScanCacheBatchExample
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    table = new HTable(conf, "testtable");

    // vv ScanCacheBatchExample
    scan(1, 1);
    scan(200, 1);
    scan(2000, 100); // co ScanCacheBatchExample-3-Test Test various combinations.
    scan(2, 100);
    scan(2, 10);
    scan(5, 100);
    scan(5, 20);
    scan(10, 10);
  }
  // ^^ ScanCacheBatchExample
}
