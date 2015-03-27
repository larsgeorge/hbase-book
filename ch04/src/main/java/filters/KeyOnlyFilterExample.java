package filters;

// cc KeyOnlyFilterExample Only returns the first found cell from each row
import java.io.IOException;

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
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

public class KeyOnlyFilterExample {

  private static Table table;

  private static void scan(Filter filter) throws IOException {
    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ KeyOnlyFilterExample
    System.out.println("Results of scan:");
    // vv KeyOnlyFilterExample
    int rowCount = 0;
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " + (
          cell.getValueLength() > 0 ?
            Bytes.toInt(cell.getValueArray(), cell.getValueOffset(),
              cell.getValueLength()) : "n/a" ));
      }
      rowCount++;
    }
    System.out.println("Total num of rows: " + rowCount);
    scanner.close();
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTableRandom("testtable", /* row */ 1, 5, 0,
       /* col */ 1, 30, 0,  /* val */ 0, 10000, 0, true, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    table = connection.getTable(TableName.valueOf("testtable"));
    System.out.println("Scan #1");
    // vv KeyOnlyFilterExample
    Filter filter1 = new KeyOnlyFilter();
    scan(filter1);
    // ^^ KeyOnlyFilterExample
    System.out.println("Scan #2");
    // vv KeyOnlyFilterExample
    Filter filter2 = new KeyOnlyFilter(true);
    scan(filter2);
    // ^^ KeyOnlyFilterExample
  }
}
