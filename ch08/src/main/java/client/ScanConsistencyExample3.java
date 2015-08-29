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

// cc ScanConsistencyExample3 Checks the scans behavior across regions and concurrent changes
public class ScanConsistencyExample3 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "master-1.internal.larsgeorge.com," +
      "master-2.internal.larsgeorge.com,master-3.internal.larsgeorge.com");
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv ScanConsistencyExample3
    /*[*/byte[][] regions = new byte[][] { Bytes.toBytes("row-5") };
    helper.createTable("testtable", regions, "colfam1");/*]*/

    // ^^ ScanConsistencyExample3
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 9, 1, "colfam1");

    System.out.println("Table before the operations:");
    helper.dump("testtable");

    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf("testtable");
    Table table = connection.getTable(tableName);

    // vv ScanConsistencyExample3
    Scan scan = new Scan();
    scan.setCaching(1);
    ResultScanner scanner = table.getScanner(scan);

    // ^^ ScanConsistencyExample3
    System.out.println("Starting scan, reading one row...");
    // vv ScanConsistencyExample3
    Result result = scanner.next();
    helper.dumpResult(result);

    // ^^ ScanConsistencyExample3
    System.out.println("Applying mutations...");
    // vv ScanConsistencyExample3
    Put put = new Put(Bytes.toBytes("row-7"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"),
      Bytes.toBytes("val-999"));
    table.put(put);

    Delete delete = new Delete(Bytes.toBytes("row-8"));
    table.delete(delete);

    // ^^ ScanConsistencyExample3
    System.out.println("Resuming original scan...");
    // vv ScanConsistencyExample3
    for (Result result2 : scanner) {
      helper.dumpResult(result2);
    }
    scanner.close();

    // ^^ ScanConsistencyExample3
    table.close();
    connection.close();
    helper.close();
  }
}
