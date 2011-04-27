package filters;

// cc CustomFilterExample Example using a custom filter
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, 2, false, "colfam1");

    HTable table = new HTable(conf, "testtable");

    // vv CustomFilterExample
    List<Filter> filters = new ArrayList<Filter>();

    Filter filter1 = new CustomFilter(Bytes.toBytes("val-05.05"));
    filters.add(filter1);

    Filter filter2 = new CustomFilter(Bytes.toBytes("val-02.07"));
    filters.add(filter2);

    Filter filter3 = new CustomFilter(Bytes.toBytes("val-09.00"));
    filters.add(filter3);

    FilterList filterList = new FilterList(
      FilterList.Operator.MUST_PASS_ONE, filters);

    Scan scan = new Scan();
    scan.setFilter(filterList);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ CustomFilterExample
    System.out.println("Results of scan:");
    // vv CustomFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " +
          Bytes.toString(kv.getValue()));
      }
    }
    scanner.close();
    // ^^ CustomFilterExample
  }
}
