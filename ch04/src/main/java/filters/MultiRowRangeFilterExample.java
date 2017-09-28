package filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

import util.HBaseHelper;

// cc MultiRowRangeFilterExample Example using the multi-row-range filter
public class MultiRowRangeFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
//    if (!helper.existsTable("testtable")) {
      helper.dropTable("testtable");
      helper.createTable("testtable", "colfam1");
      System.out.println("Adding rows to table...");
      helper.fillTable("testtable", 1, 100, 10, 3, false, "colfam1");
//    }

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv MultiRowRangeFilterExample
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes("row-010"), true,
      Bytes.toBytes("row-020"), false));
    ranges.add(new RowRange(Bytes.toBytes("row-050"), true,
      Bytes.toBytes("row-090"), true));
    ranges.add(new RowRange(Bytes.toBytes("row-096"), true,
      Bytes.toBytes("row-097"), false));

    Filter filter = new MultiRowRangeFilter(ranges);

    Scan scan = new Scan(Bytes.toBytes("row-005"), Bytes.toBytes("row-110"));
    scan.setFilter(filter);

    ResultScanner scanner = table.getScanner(scan);
    // ^^ MultiRowRangeFilterExample
    System.out.println("Results of scan:");
    // vv MultiRowRangeFilterExample
    int numRows = 0;
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
      numRows++;
    }
    // ^^ MultiRowRangeFilterExample
    System.out.println("Number of rows: " + numRows);
    // vv MultiRowRangeFilterExample
    scanner.close();
  }
}
