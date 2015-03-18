package client;

// cc GetListErrorExample Example trying to read an erroneous column family
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class GetListErrorExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    if (!helper.existsTable("testtable")) {
      helper.createTable("testtable", "colfam1");
    }
    
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    byte[] cf1 = Bytes.toBytes("colfam1");
    byte[] qf1 = Bytes.toBytes("qual1");
    byte[] qf2 = Bytes.toBytes("qual2");
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");

    // vv GetListErrorExample
    List<Get> gets = new ArrayList<Get>();

    Get get1 = new Get(row1);
    get1.addColumn(cf1, qf1);
    gets.add(get1);

    Get get2 = new Get(row2);
    get2.addColumn(cf1, qf1); // co GetListErrorExample-1-AddGets Add the Get instances to the list.
    gets.add(get2);

    Get get3 = new Get(row2);
    get3.addColumn(cf1, qf2);
    gets.add(get3);

    Get get4 = new Get(row2);
    /*[*/get4.addColumn(Bytes.toBytes("BOGUS"),/*]*/ qf2);
    gets.add(get4); // co GetListErrorExample-2-AddBogus Add the bogus column family get.

    Result[] results = table.get(gets); // co GetListErrorExample-3-Error An exception is thrown and the process is aborted.

    System.out.println("Result count: " + results.length); // co GetListErrorExample-4-SOUT This line will never reached!
    // ^^ GetListErrorExample
    table.close();
    connection.close();
    helper.close();
  }
}
