package client;

// cc CheckAndDeleteExample Example application using the atomic compare-and-set operations
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class CheckAndDeleteExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.put("testtable",
      new String[] { "row1" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual1", "qual2", "qual2", "qual3", "qual3" },
      new long[]   { 1, 2, 3, 4, 5, 6 },
      new String[] { "val1", "val2", "val3", "val4", "val5", "val6" });
    System.out.println("Before delete call...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);

    HTable table = new HTable(conf, "testtable");

    // vv CheckAndDeleteExample
    Delete delete1 = new Delete(Bytes.toBytes("row1"));
    delete1.deleteColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3")); // co CheckAndDeleteExample-1-Delete1 Create a new Delete instance.

    boolean res1 = table.checkAndDelete(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), null, delete1); // co CheckAndDeleteExample-2-CAS1 Check if column does not exist and perform optional delete operation.
    System.out.println("Delete successful: " + res1); // co CheckAndDeleteExample-3-SOUT1 Print out the result, should be "Delete successful: false".

    Delete delete2 = new Delete(Bytes.toBytes("row1"));
    delete2.deleteColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3")); // co CheckAndDeleteExample-4-Delete2 Delete checked column manually.
    table.delete(delete2);

    boolean res2 = table.checkAndDelete(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), null, delete1); // co CheckAndDeleteExample-5-CAS2 Attempt to delete same cell again.
    System.out.println("Delete successful: " + res2); // co CheckAndDeleteExample-6-SOUT2 Print out the result, should be "Delete successful: true" as the column now already exists.

    Delete delete3 = new Delete(Bytes.toBytes("row2"));
    delete3.deleteFamily(Bytes.toBytes("colfam1")); // co CheckAndDeleteExample-7-Delete3 Create yet another Delete instance, but using a different row.

    try{
      boolean res4 = table.checkAndDelete(Bytes.toBytes("row1"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), // co CheckAndDeleteExample-8-CAS4 Try to delete while checking a different row.
        Bytes.toBytes("val1"), delete3);
      System.out.println("Delete successful: " + res4); // co CheckAndDeleteExample-9-SOUT4 We will not get here as an exception is thrown beforehand!
    } catch (Exception e) {
      System.err.println("Error: " + e);
    }
    // ^^ CheckAndDeleteExample
    System.out.println("After delete call...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);
  }
}
