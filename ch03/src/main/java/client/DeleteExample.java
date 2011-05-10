package client;

// cc DeleteExample Example application deleting data from HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class DeleteExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    HTable table = new HTable(conf, "testtable");
    helper.put("testtable",
      new String[] { "row1" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" },
      new long[]   { 1, 2, 3, 4, 5, 6 },
      new String[] { "val1", "val2", "val3", "val4", "val5", "val6" });
    System.out.println("Before delete call...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);
    // vv DeleteExample
    Delete delete = new Delete(Bytes.toBytes("row1")); // co DeleteExample-1-NewDel Create delete with specific row.

    delete.setTimestamp(1); // co DeleteExample-2-SetTS Set timestamp for row deletes.

    delete.deleteColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co DeleteExample-3-DelColNoTS Delete the latest version only in one column.
    delete.deleteColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 5); // co DeleteExample-4-DelColTS Delete specific version in one column.

    delete.deleteColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co DeleteExample-5-DelColsNoTS Delete all versions in one column.
    delete.deleteColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"), 6); // co DeleteExample-6-DelColsTS Delete the given and all older versions in one column.

    delete.deleteFamily(Bytes.toBytes("colfam1")); // co DeleteExample-7-AddCol Delete entire family, all columns and versions.
    delete.deleteFamily(Bytes.toBytes("colfam1"), 3); // co DeleteExample-8-AddCol Delete the given and all older versions in the entire column family, i.e., from all columns therein.

    table.delete(delete); // co DeleteExample-9-DoDel Delete the data from the HBase table.

    table.close();
    // ^^ DeleteExample
    System.out.println("After delete call...");
    helper.dump("testtable", new String[] { "row1" }, null, null);
  }
}
