package filters;

// cc ValueNotNullFilterExample Example using the single value based filter in combination

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class ValueNotNullFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 5, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    Put put = new Put(Bytes.toBytes("row-5"))
      .addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-6"),
        Bytes.toBytes("val-6"));
    table.put(put);
    put = new Put(Bytes.toBytes("row-8"))
      .addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-6"),
        Bytes.toBytes("val-6"));
    table.put(put);

    Delete delete = new Delete(Bytes.toBytes("row-5"))
      .addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-3"));
    table.delete(delete);

    // vv ValueNotNullFilterExample
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
      Bytes.toBytes("colfam1"), Bytes.toBytes("col-6"),
      CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
    filter.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
      Bytes.toBytes("colfam1"), Bytes.toBytes("col-3"),
      CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
    filter2.setFilterIfMissing(true);

    FilterList list = new FilterList();
    list.addFilter(filter);
    list.addFilter(filter2);

    Scan scan = new Scan();
    scan.setFilter(list);

    ResultScanner scanner = table.getScanner(scan);
    // ^^ ValueNotNullFilterExample
    System.out.println("Results of scan:");
    // vv ValueNotNullFilterExample
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
    }
    scanner.close();
    // ^^ ValueNotNullFilterExample
    table.close();
    connection.close();
  }
}
