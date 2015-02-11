package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class CRUDExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    if (!helper.existsTable("testtable")) {
      helper.createTable("testtable", "fam-A", "fam-B");
    }

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    Put put = new Put(Bytes.toBytes("myrow-1"));
    put.add(Bytes.toBytes("fam-A"), Bytes.toBytes("col-A"),
      Bytes.toBytes("val-1"));
    put.add(Bytes.toBytes("fam-B"), Bytes.toBytes("col-B"),
      Bytes.toBytes("val-2"));
    table.put(put);

    Get get = new Get(Bytes.toBytes("myrow-1"));
    get.addColumn(Bytes.toBytes("fam-A"), Bytes.toBytes("col-A"));
    Result result = table.get(get);
    System.out.println(result);
    byte[] val = result.getValue(Bytes.toBytes("fam-A"),
      Bytes.toBytes("col-A"));
    System.out.println("Value: " + Bytes.toString(val));

    Delete delete = new Delete(Bytes.toBytes("myrow-1"));
    delete.deleteColumns(Bytes.toBytes("fam-A"), Bytes.toBytes("col-A"));
    table.delete(delete);

    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    for (Result result2 : scanner) {
      System.out.println(result2);
    }

    table.close();
  }
}
