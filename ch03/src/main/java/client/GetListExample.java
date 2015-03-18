package client;

// cc GetListExample Example of retrieving data from HBase using lists of Get instances
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class GetListExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    if (!helper.existsTable("testtable")) {
      helper.createTable("testtable", "colfam1");
    }

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv GetListExample
    byte[] cf1 = Bytes.toBytes("colfam1");
    byte[] qf1 = Bytes.toBytes("qual1");
    byte[] qf2 = Bytes.toBytes("qual2"); // co GetListExample-1-Prepare Prepare commonly used byte arrays.
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");

    List<Get> gets = new ArrayList<Get>();  // co GetListExample-2-CreateList Create a list that holds the Get instances.

    Get get1 = new Get(row1);
    get1.addColumn(cf1, qf1);
    gets.add(get1);

    Get get2 = new Get(row2);
    get2.addColumn(cf1, qf1); // co GetListExample-3-AddGets Add the Get instances to the list.
    gets.add(get2);

    Get get3 = new Get(row2);
    get3.addColumn(cf1, qf2);
    gets.add(get3);

    Result[] results = table.get(gets); // co GetListExample-4-DoGet Retrieve rows with selected columns from HBase.

    System.out.println("First iteration...");
    for (Result result : results) {
      String row = Bytes.toString(result.getRow());
      System.out.print("Row: " + row + " ");
      byte[] val = null;
      if (result.containsColumn(cf1, qf1)) { // co GetListExample-5-GetValue1 Iterate over results and check what values are available.
        val = result.getValue(cf1, qf1);
        System.out.println("Value: " + Bytes.toString(val));
      }
      if (result.containsColumn(cf1, qf2)) {
        val = result.getValue(cf1, qf2);
        System.out.println("Value: " + Bytes.toString(val));
      }
    }

    System.out.println("Second iteration...");
    for (Result result : results) {
      for (Cell cell : result.listCells()) { // co GetListExample-6-GetValue2 Iterate over results again, printing out all values.
        System.out.println(
          "Row: " + Bytes.toString(
            cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + // co GetListExample-7-GetValue2 Two different ways to access the cell data.
          " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
      }
    }

    System.out.println("Third iteration...");
    for (Result result : results) {
      System.out.println(result);
    }
    // ^^ GetListExample
    table.close();
    connection.close();
    helper.close();
  }
}
