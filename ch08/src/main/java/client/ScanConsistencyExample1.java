package client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc ScanConsistencyExample1 Checks the scans behavior during concurrent modifications
public class ScanConsistencyExample1 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 5, 2, "colfam1");

    System.out.println("Table before the operations:");
    helper.dump("testtable");

    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf("testtable");
    Table table = connection.getTable(tableName);

    // vv ScanConsistencyExample1
    Scan scan = new Scan();
    scan.setCaching(1); // co ScanConsistencyExample1-1-ConfScan Configure scan to iterate over each row separately.
    ResultScanner scanner = table.getScanner(scan);

    // ^^ ScanConsistencyExample1
    System.out.println("Starting scan, reading one row...");
    // vv ScanConsistencyExample1
    Result result = scanner.next();
    helper.dumpResult(result);

    // ^^ ScanConsistencyExample1
    System.out.println("Applying mutations...");
    // vv ScanConsistencyExample1
    Put put = new Put(Bytes.toBytes("row-3"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"),
      Bytes.toBytes("val-999"));
    table.put(put); // co ScanConsistencyExample1-2-Put Update a later row with a new value.

    Delete delete = new Delete(Bytes.toBytes("row-4"));
    table.delete(delete); // co ScanConsistencyExample1-3-Delete Remove an entire row, that is located at the end of the scan.

    // ^^ ScanConsistencyExample1
    System.out.println("Resuming original scan...");
    // vv ScanConsistencyExample1
    for (Result result2 : scanner) {
      helper.dumpResult(result2); // co ScanConsistencyExample1-4-Scan Scan the rest of the table to see if the mutations are visible.
    }
    scanner.close();

    // ^^ ScanConsistencyExample1
    System.out.println("Print table under new scanner...");
    // vv ScanConsistencyExample1
    helper.dump("testtable"); // co ScanConsistencyExample1-5-Dump Print the entire table again, with a new scanner instance.
    // ^^ ScanConsistencyExample1
    table.close();
    connection.close();
    helper.close();
  }
}
