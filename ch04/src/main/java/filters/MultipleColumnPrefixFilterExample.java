package filters;

// cc MultipleColumnPrefixFilterExample Example filtering by column prefix
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
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class MultipleColumnPrefixFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 30, 50, 0, true, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));
    // vv MultipleColumnPrefixFilterExample
    Filter filter = new MultipleColumnPrefixFilter(new byte[][] {
      Bytes.toBytes("col-1"), Bytes.toBytes("col-2")
    });

    Scan scan = new Scan()
      .setRowPrefixFilter(Bytes.toBytes("row-1")) // co MultipleColumnPrefixFilterExample-1-Row Limit to rows starting with a specific prefix.
      .setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ MultipleColumnPrefixFilterExample
    System.out.println("Results of scan:");
    // vv MultipleColumnPrefixFilterExample
    for (Result result : scanner) {
      System.out.print(Bytes.toString(result.getRow()) + ": ");
      for (Cell cell : result.rawCells()) {
        System.out.print(Bytes.toString(cell.getQualifierArray(),
          cell.getQualifierOffset(), cell.getQualifierLength()) + ", ");
      }
      System.out.println();
    }
    scanner.close();
    // ^^ MultipleColumnPrefixFilterExample
  }
}
