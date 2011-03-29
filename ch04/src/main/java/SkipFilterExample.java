// cc SkipFilterExample Example paginating through columns in a row
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SkipFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 30, 1, 2, true, "colfam1");

    HTable table = new HTable(conf, "testtable");

    // vv SkipFilterExample
//    Filter filter = new SkipFilter(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
//      new BinaryComparator(Bytes.toBytes("row-15"))));
    Filter filter = new SkipFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes("val-10.00"))));

    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ SkipFilterExample
    System.out.println("Results of scan:");
    // vv SkipFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " +
          Bytes.toString(kv.getValue()));
      }
    }    scanner.close();
    // ^^ SkipFilterExample
  }
}
