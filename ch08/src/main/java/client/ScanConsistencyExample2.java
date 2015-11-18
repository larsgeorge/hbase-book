package client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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

// cc ScanConsistencyExample2 Checks the scans behavior during concurrent modifications
public class ScanConsistencyExample2 {

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

    Scan scan = new Scan();
    scan.setCaching(1);
    ResultScanner scanner = table.getScanner(scan);

    System.out.println("Starting scan, reading one row...");
    Result result = scanner.next();
    helper.dumpResult(result);

    System.out.println("Applying mutations...");
    Put put = new Put(Bytes.toBytes("row-3"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"),
      Bytes.toBytes("val-999"));
    table.put(put);

    Delete delete = new Delete(Bytes.toBytes("row-4"));
    table.delete(delete);

    System.out.println("Flushing and splitting table...");
    // vv ScanConsistencyExample2
    Admin admin = connection.getAdmin();
    admin.flush(tableName); // co ScanConsistencyExample2-1-Flush Flush table and wait a little while for the operation to complete.
    try {
      Thread.currentThread().sleep(4000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    admin.split(tableName, Bytes.toBytes("row-3")); // co ScanConsistencyExample2-2-Split Split the table and wait until split operation has completed.
    while (admin.getTableRegions(tableName).size() == 1) {  }

    // ^^ ScanConsistencyExample2
    System.out.println("Resuming original scan...");
    // vv ScanConsistencyExample2
    for (Result result2 : scanner) {
      helper.dumpResult(result2);
    }
    scanner.close();

    // ^^ ScanConsistencyExample2
    System.out.println("Print table under new scanner...");
    helper.dump("testtable");
    table.close();
    connection.close();
    helper.close();
  }
}
