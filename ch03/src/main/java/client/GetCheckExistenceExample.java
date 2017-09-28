package client;

// cc GetCheckExistenceExample Checks for the existence of specific data
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class GetCheckExistenceExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv GetCheckExistenceExample
    List<Put> puts = new ArrayList<Put>();
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    puts.add(put1);
    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    puts.add(put2);
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val3"));
    puts.add(put3);
    table.put(puts); // co GetCheckExistenceExample-1-Puts Insert two rows into the table.

    Get get1 = new Get(Bytes.toBytes("row2"));
    get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    get1.setCheckExistenceOnly(true);
    Result result1 = table.get(get1); // co GetCheckExistenceExample-2-Get1 Check first with existing data.

    byte[] val = result1.getValue(Bytes.toBytes("colfam1"),
      Bytes.toBytes("qual1"));

    System.out.println("Get 1 Exists: " + result1.getExists());
    System.out.println("Get 1 Size: " + result1.size()); // co GetCheckExistenceExample-3-Result1 Exists is "true", while no cell was actually returned.
    System.out.println("Get 1 Value: " + Bytes.toString(val));

    Get get2 = new Get(Bytes.toBytes("row2"));
    get2.addFamily(Bytes.toBytes("colfam1")); // co GetCheckExistenceExample-4-Get2 Check for an entire family to exist.
    get2.setCheckExistenceOnly(true);
    Result result2 = table.get(get2);

    System.out.println("Get 2 Exists: " + result2.getExists());
    System.out.println("Get 2 Size: " + result2.size());

    Get get3 = new Get(Bytes.toBytes("row2"));
    get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999")); // co GetCheckExistenceExample-5-Get3 Check for a non-existent column.
    get3.setCheckExistenceOnly(true);
    Result result3 = table.get(get3);

    System.out.println("Get 3 Exists: " + result3.getExists());
    System.out.println("Get 3 Size: " + result3.size());

    Get get4 = new Get(Bytes.toBytes("row2"));
    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999")); // co GetCheckExistenceExample-6-Get4 Check for an existent, and non-existent column.
    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    get4.setCheckExistenceOnly(true);
    Result result4 = table.get(get4);

    System.out.println("Get 4 Exists: " + result4.getExists()); // co GetCheckExistenceExample-7-Result4 Exists is "true" because some data exists.
    System.out.println("Get 4 Size: " + result4.size());

    // ^^ GetCheckExistenceExample
    table.close();
    connection.close();
    helper.close();
  }
}
