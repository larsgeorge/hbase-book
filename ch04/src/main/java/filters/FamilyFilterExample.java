package filters;

// cc FamilyFilterExample Example using a filter to include only specific column families
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class FamilyFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2", "colfam3", "colfam4");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 2, "colfam1", "colfam2", "colfam3", "colfam4");

    HTable table = new HTable(conf, "testtable");

    // vv FamilyFilterExample
    Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS, // co FamilyFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
      new BinaryComparator(Bytes.toBytes("colfam3")));

    Scan scan = new Scan();
    scan.setFilter(filter1);
    ResultScanner scanner = table.getScanner(scan); // co FamilyFilterExample-2-Scan Scan over table while applying the filter.
    // ^^ FamilyFilterExample
    System.out.println("Scanning table... ");
    // vv FamilyFilterExample
    for (Result result : scanner) {
      System.out.println(result);
    }
    scanner.close();

    Get get1 = new Get(Bytes.toBytes("row-5"));
    get1.setFilter(filter1);
    Result result1 = table.get(get1); // co FamilyFilterExample-3-Get Get a row while applying the same filter.
    System.out.println("Result of get(): " + result1);

    Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes("colfam3")));
    Get get2 = new Get(Bytes.toBytes("row-5")); // co FamilyFilterExample-4-Mismatch Create a filter on one column family while trying to retrieve another.
    get2.addFamily(Bytes.toBytes("colfam1"));
    get2.setFilter(filter2);
    Result result2 = table.get(get2); // co FamilyFilterExample-5-Get2 Get the same row while applying the new filter, this will return "NONE".
    System.out.println("Result of get(): " + result2);
    // ^^ FamilyFilterExample
  }
}
