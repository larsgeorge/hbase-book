package filters;

// cc ValueFilterExample Example using the value based filter
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class ValueFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    HTable table = new HTable(conf, "testtable");

    // vv ValueFilterExample
    Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, // co ValueFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
      new SubstringComparator(".4"));

    Scan scan = new Scan();
    scan.setFilter(filter); // co ValueFilterExample-2-SetFilter Set filter for the scan.
    ResultScanner scanner = table.getScanner(scan);
    // ^^ ValueFilterExample
    System.out.println("Results of scan:");
    // vv ValueFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " + // co ValueFilterExample-3-Print1 Print out value to check that filter works.
          Bytes.toString(kv.getValue()));
      }
    }
    scanner.close();

    Get get = new Get(Bytes.toBytes("row-5"));
    get.setFilter(filter); // co ValueFilterExample-4-SetFilter2 Assign same filter to Get instance.
    Result result = table.get(get);
    // ^^ ValueFilterExample
    System.out.println("Result of get: ");
    // vv ValueFilterExample
    for (KeyValue kv : result.raw()) {
      System.out.println("KV: " + kv + ", Value: " +
        Bytes.toString(kv.getValue()));
    }
    // ^^ ValueFilterExample
  }
}
