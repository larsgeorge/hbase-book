package filters;

// cc FilterListExample Example of using a filter list to combine single purpose filters
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class FilterListExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 5, 2, true, false, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));
    // vv FilterListExample
    List<Filter> filters = new ArrayList<Filter>();

    Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
      new BinaryComparator(Bytes.toBytes("row-03")));
    filters.add(filter1);

    Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
      new BinaryComparator(Bytes.toBytes("row-06")));
    filters.add(filter2);

    Filter filter3 = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator("col-0[03]"));
    filters.add(filter3);

    FilterList filterList1 = new FilterList(filters);

    Scan scan = new Scan();
    scan.setFilter(filterList1);
    ResultScanner scanner1 = table.getScanner(scan);
    // ^^ FilterListExample
    System.out.println("Results of scan #1 - MUST_PASS_ALL:");
    int n = 0;
    // vv FilterListExample
    for (Result result : scanner1) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
        // ^^ FilterListExample
        n++;
        // vv FilterListExample
      }
    }
    scanner1.close();

    FilterList filterList2 = new FilterList(
      FilterList.Operator.MUST_PASS_ONE, filters);

    scan.setFilter(filterList2);
    ResultScanner scanner2 = table.getScanner(scan);
    // ^^ FilterListExample
    System.out.println("Total cell count for scan #1: " + n);
    n = 0;
    System.out.println("Results of scan #2 - MUST_PASS_ONE:");
    // vv FilterListExample
    for (Result result : scanner2) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
        // ^^ FilterListExample
        n++;
        // vv FilterListExample
      }
    }
    scanner2.close();
    // ^^ FilterListExample
    System.out.println("Total cell count for scan #2: " + n);
  }
}
