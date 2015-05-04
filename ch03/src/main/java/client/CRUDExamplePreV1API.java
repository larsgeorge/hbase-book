package client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

// cc CRUDExamplePreV1API Example application using all of the basic access methods (before v1.0)
@SuppressWarnings("deprecation") // because of old API usage
public class CRUDExamplePreV1API {

  public static void main(String[] args) throws IOException {
    // vv CRUDExamplePreV1API
    Configuration conf = HBaseConfiguration.create();

    // ^^ CRUDExamplePreV1API
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");

    // vv CRUDExamplePreV1API
    HTable table = new HTable(conf, "testtable");

    Put put = new Put(Bytes.toBytes("row1"));
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    put.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val2"));
    table.put(put);

    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    for (Result result2 : scanner) {
      System.out.println("Scan 1: " + result2);
    }

    Get get = new Get(Bytes.toBytes("row1"));
    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    Result result = table.get(get);
    System.out.println("Get result: " + result);
    byte[] val = result.getValue(Bytes.toBytes("colfam1"),
      Bytes.toBytes("qual1"));
    System.out.println("Value only: " + Bytes.toString(val));

    Delete delete = new Delete(Bytes.toBytes("row1"));
    delete.deleteColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    table.delete(delete);

    Scan scan2 = new Scan();
    ResultScanner scanner2 = table.getScanner(scan2);
    for (Result result2 : scanner2) {
      System.out.println("Scan2: " + result2);
    }
    // ^^ CRUDExamplePreV1API
  }
}
