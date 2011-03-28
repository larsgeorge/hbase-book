// cc TimestampFilterExample Example filtering data by timestamps
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TimestampFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 100, 20, true, "colfam1");

    HTable table = new HTable(conf, "testtable");

    // vv TimestampFilterExample
    List<Long> ts = new ArrayList<Long>();
    ts.add(new Long(5));
    ts.add(new Long(10));
    ts.add(new Long(15));
    Filter filter = new TimestampsFilter(ts);

    Scan scan = new Scan();
    scan.setFilter(filter);
    scan.setTimeRange(8, 12);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ TimestampFilterExample
    System.out.println("Results of scan:");
    // vv TimestampFilterExample
    for (Result result : scanner) {
      System.out.println(result);
    }
    scanner.close();
    // ^^ TimestampFilterExample
  }
}
