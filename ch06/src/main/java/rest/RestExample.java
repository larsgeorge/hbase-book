package rest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc RestExample Example of using the REST client classes
public class RestExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 100, 10, "colfam1");

    // vv RestExample
    Cluster cluster = new Cluster();
    cluster.add("localhost", 8080); // co RestExample-1-Cluster Set up a cluster list adding all known REST server hosts.

    Client client = new Client(cluster); // co RestExample-2-Client Create the client handling the HTTP communication.

    RemoteHTable table = new RemoteHTable(client, "testtable"); // co RestExample-3-Table Create a remote table instance, wrapping the REST access into a familiar interface.

    Get get = new Get(Bytes.toBytes("row-30")); // co RestExample-4-Get Perform a get operation as if it were a direct HBase connection.
    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-3"));
    Result result1 = table.get(get);

    System.out.println("Get result1: " + result1);

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("row-10"));
    scan.setStopRow(Bytes.toBytes("row-15"));
    scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"));
    ResultScanner scanner = table.getScanner(scan); // co RestExample-5-Scan Scan the table, again, the same approach as if using the native Java API.

    for (Result result2 : scanner) {
      System.out.println("Scan row[" + Bytes.toString(result2.getRow()) +
        "]: " + result2);
    }
    // ^^ RestExample
  }
}
