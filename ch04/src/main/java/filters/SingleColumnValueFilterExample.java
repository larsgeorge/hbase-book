package filters;

// cc SingleColumnValueFilterExample Example using a filter to return only rows with a given value in a given column
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class SingleColumnValueFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    HTable table = new HTable(conf, "testtable");

    // vv SingleColumnValueFilterExample
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
      Bytes.toBytes("colfam1"),
      Bytes.toBytes("col-5"),
      CompareFilter.CompareOp.NOT_EQUAL,
      new SubstringComparator("val-5"));
    filter.setFilterIfMissing(true);

    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ SingleColumnValueFilterExample
    System.out.println("Results of scan:");
    // vv SingleColumnValueFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " +
          Bytes.toString(kv.getValue()));
      }
    }
    scanner.close();

    Get get = new Get(Bytes.toBytes("row-6"));
    get.setFilter(filter);
    Result result = table.get(get);
    System.out.println("Result of get: ");
    for (KeyValue kv : result.raw()) {
      System.out.println("KV: " + kv + ", Value: " +
        Bytes.toString(kv.getValue()));
    }
    // ^^ SingleColumnValueFilterExample
  }
}
