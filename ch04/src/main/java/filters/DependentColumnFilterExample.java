package filters;

// cc DependentColumnFilterExample Example using a filter to include only specific column families
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class DependentColumnFilterExample {

  private static HTable table = null;

  // vv DependentColumnFilterExample
  private static void filter(boolean drop,
      CompareFilter.CompareOp operator,
      WritableByteArrayComparable comparator)
  throws IOException {
    Filter filter;
    if (comparator != null) {
      filter = new DependentColumnFilter(Bytes.toBytes("colfam1"), // co DependentColumnFilterExample-1-CreateFilter Create the filter with various options.
        Bytes.toBytes("col-5"), drop, operator, comparator);
    } else {
      filter = new DependentColumnFilter(Bytes.toBytes("colfam1"),
        Bytes.toBytes("col-5"), drop);

    }

    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ DependentColumnFilterExample
    System.out.println("Results of scan:");
    // vv DependentColumnFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " +
          Bytes.toString(kv.getValue()));
      }
    }
    scanner.close();

    Get get = new Get(Bytes.toBytes("row-5"));
    get.setFilter(filter);
    Result result = table.get(get);
    // ^^ DependentColumnFilterExample
    System.out.println("Result of get: ");
    // vv DependentColumnFilterExample
    for (KeyValue kv : result.raw()) {
      System.out.println("KV: " + kv + ", Value: " +
        Bytes.toString(kv.getValue()));
    }
    // ^^ DependentColumnFilterExample
    System.out.println("");
    // vv DependentColumnFilterExample
  }

  public static void main(String[] args) throws IOException {
    // ^^ DependentColumnFilterExample
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, true, "colfam1", "colfam2");

    table = new HTable(conf, "testtable");

    // vv DependentColumnFilterExample
    filter(true, CompareFilter.CompareOp.NO_OP, null);
    filter(false, CompareFilter.CompareOp.NO_OP, null); // co DependentColumnFilterExample-2-Filter Call filter method with various options.
    filter(true, CompareFilter.CompareOp.EQUAL,
      new BinaryPrefixComparator(Bytes.toBytes("val-5")));
    filter(false, CompareFilter.CompareOp.EQUAL,
      new BinaryPrefixComparator(Bytes.toBytes("val-5")));
    filter(true, CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(".*\\.5"));
    filter(false, CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(".*\\.5"));
  }
  // ^^ DependentColumnFilterExample
}
