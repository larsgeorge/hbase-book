package filters;

// cc FirstKeyValueMatchingQualifiersFilterExample Returns all columns, or up to the first found reference qualifier, for each row
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

public class FirstKeyValueMatchingQualifiersFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTableRandom("testtable", /* row */ 1, 50, 0,
       /* col */ 1, 10, 0,  /* val */ 0, 100, 0, true, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));
    // vv FirstKeyValueMatchingQualifiersFilterExample
    Set<byte[]> quals = new HashSet<byte[]>();
    quals.add(Bytes.toBytes("col-2"));
    quals.add(Bytes.toBytes("col-4"));
    quals.add(Bytes.toBytes("col-6"));
    quals.add(Bytes.toBytes("col-8"));
    Filter filter = new FirstKeyValueMatchingQualifiersFilter(quals);

    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ FirstKeyValueMatchingQualifiersFilterExample
    System.out.println("Results of scan:");
    // vv FirstKeyValueMatchingQualifiersFilterExample
    int rowCount = 0;
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
      rowCount++;
    }
    System.out.println("Total num of rows: " + rowCount);
    scanner.close();
    // ^^ FirstKeyValueMatchingQualifiersFilterExample
  }
}
