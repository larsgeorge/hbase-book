package filters;

// cc InclusiveStopFilterExample Example using a filter to include a stop row
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class InclusiveStopFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 100, 1, "colfam1");

    HTable table = new HTable(conf, "testtable");

    // vv InclusiveStopFilterExample
    Filter filter = new InclusiveStopFilter(Bytes.toBytes("row-5"));

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("row-3"));
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ InclusiveStopFilterExample
    System.out.println("Results of scan:");
    // vv InclusiveStopFilterExample
    for (Result result : scanner) {
      System.out.println(result);
    }
    scanner.close();
    // ^^ InclusiveStopFilterExample
  }
}
