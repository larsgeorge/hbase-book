package client;

// cc CheckAndMutateExample Example using the atomic check-and-mutate operations
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class CheckAndMutateExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 100, "colfam1", "colfam2");
    helper.put("testtable",
      new String[] { "row1" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual2", "qual3" },
      new long[]   { 1, 2, 3 },
      new String[] { "val1", "val2", "val3" });
    System.out.println("Before check and mutate calls...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    //BinaryComparator bc = new BinaryComparator(Bytes.toBytes("val1"));
    //System.out.println(bc.compareTo(Bytes.toBytes("val2")));

    // vv CheckAndMutateExample
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      4, Bytes.toBytes("val99"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"),
      4, Bytes.toBytes("val100"));

    Delete delete = new Delete(Bytes.toBytes("row1"));
    delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));

    RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
    mutations.add(put);
    mutations.add(delete);

    boolean res1 = table.checkAndMutate(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"),
      CompareFilter.CompareOp.LESS, Bytes.toBytes("val1"), mutations); // co CheckAndMutateExample-1-Check1 Check if the column contains a value that is less than "val1". Here we receive "false" as the value is equal, but not lesser.
    System.out.println("Mutate 1 successful: " + res1);

    Put put2 = new Put(Bytes.toBytes("row1"));
    put2.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"), // co CheckAndMutateExample-2-Put1 Update the checked column to have a value greater than what we check for.
      4, Bytes.toBytes("val2"));
    table.put(put2);

    boolean res2 = table.checkAndMutate(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"),
      CompareFilter.CompareOp.LESS, Bytes.toBytes("val1"), mutations); // co CheckAndMutateExample-3-Check2 Now "val1" is less than "val2" (binary comparison) and we expect "true" to be printed on the console.
    System.out.println("Mutate 2 successful: " + res2);

    // ^^ CheckAndMutateExample
    System.out.println("After check and mutate calls...");
    helper.dump("testtable", new String[]{"row1"}, null, null);
    table.close();
    connection.close();
    helper.close();
  }
}
