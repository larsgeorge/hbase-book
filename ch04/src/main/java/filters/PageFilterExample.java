package filters;

// cc PageFilterExample Example using a filter to paginate through rows
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PageFilterExample {

  // vv PageFilterExample
  private static final byte[] POSTFIX = new byte[] { 0x00 };
  // ^^ PageFilterExample

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 1000, 10, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv PageFilterExample
    Filter filter = new PageFilter(15);

    int totalRows = 0;
    byte[] lastRow = null;
    while (true) {
      Scan scan = new Scan();
      scan.setFilter(filter);
      if (lastRow != null) {
        byte[] startRow = Bytes.add(lastRow, POSTFIX);
        System.out.println("start row: " +
          Bytes.toStringBinary(startRow));
        scan.setStartRow(startRow);
      }
      ResultScanner scanner = table.getScanner(scan);
      int localRows = 0;
      Result result;
      while ((result = scanner.next()) != null) {
        System.out.println(localRows++ + ": " + result);
        totalRows++;
        lastRow = result.getRow();
      }
      scanner.close();
      if (localRows == 0) break;
    }
    System.out.println("total rows: " + totalRows);
    // ^^ PageFilterExample
  }
}
