package client;

// cc DeleteListExample Example application deleting lists of data from HBase
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class DeleteListExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 100, "colfam1", "colfam2");
    helper.put("testtable",
      new String[] { "row1" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" },
      new long[]   { 1, 2, 3, 4, 5, 6 },
      new String[] { "val1", "val2", "val3", "val4", "val5", "val6" });
    helper.put("testtable",
      new String[] { "row2" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" },
      new long[]   { 1, 2, 3, 4, 5, 6 },
      new String[] { "val1", "val2", "val3", "val4", "val5", "val6" });
    helper.put("testtable",
      new String[] { "row3" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" },
      new long[]   { 1, 2, 3, 4, 5, 6 },
      new String[] { "val1", "val2", "val3", "val4", "val5", "val6" });
    System.out.println("Before delete call...");
    helper.dump("testtable", new String[]{ "row1", "row2", "row3" }, null, null);

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv DeleteListExample
    List<Delete> deletes = new ArrayList<Delete>(); // co DeleteListExample-1-CreateList Create a list that holds the Delete instances.

    Delete delete1 = new Delete(Bytes.toBytes("row1"));
    delete1.setTimestamp(4); // co DeleteListExample-2-SetTS Set timestamp for row deletes.
    deletes.add(delete1);

    Delete delete2 = new Delete(Bytes.toBytes("row2"));
    delete2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co DeleteListExample-3-DelColNoTS Delete the latest version only in one column.
    delete2.addColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), 5); // co DeleteListExample-4-DelColsTS Delete the given and all older versions in another column.
    deletes.add(delete2);

    Delete delete3 = new Delete(Bytes.toBytes("row3"));
    delete3.addFamily(Bytes.toBytes("colfam1")); // co DeleteListExample-5-AddCol Delete entire family, all columns and versions.
    delete3.addFamily(Bytes.toBytes("colfam2"), 3); // co DeleteListExample-6-AddCol Delete the given and all older versions in the entire column family, i.e., from all columns therein.
    deletes.add(delete3);

    table.delete(deletes); // co DeleteListExample-7-DoDel Delete the data from multiple rows the HBase table.
    // ^^ DeleteListExample
    table.close();
    connection.close();
    System.out.println("After delete call...");
    helper.dump("testtable", new String[]{ "row1", "row2", "row3" }, null, null);
    helper.close();
  }
}
