package client;

// cc CheckAndPutExample Example application using the atomic compare-and-set operations
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class CheckAndPutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    HTable table = new HTable(conf, "testtable");

    // vv CheckAndPutExample
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1")); // co CheckAndPutExample-1-Put1 Create a new Put instance.

    boolean res1 = table.checkAndPut(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put1); // co CheckAndPutExample-2-CAS1 Check if column does not exist and perform optional put operation.
    System.out.println("Put applied: " + res1); // co CheckAndPutExample-3-SOUT1 Print out the result, should be "Put applied: true".

    boolean res2 = table.checkAndPut(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put1); // co CheckAndPutExample-4-CAS2 Attempt to store same cell again.
    System.out.println("Put applied: " + res2); // co CheckAndPutExample-5-SOUT2 Print out the result, should be "Put applied: false" as the column now already exists.

    Put put2 = new Put(Bytes.toBytes("row1"));
    put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val2")); // co CheckAndPutExample-6-Put2 Create another Put instance, but using a different column qualifier.

    boolean res3 = table.checkAndPut(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), // co CheckAndPutExample-7-CAS3 Store new data only if the previous data has been saved.
      Bytes.toBytes("val1"), put2);
    System.out.println("Put applied: " + res3); // co CheckAndPutExample-8-SOUT3 Print out the result, should be "Put applied: true" as the checked column already exists.

    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val3")); // co CheckAndPutExample-9-Put3 Create yet another Put instance, but using a different row.

    boolean res4 = table.checkAndPut(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), // co CheckAndPutExample-a-CAS4 Store new data while checking a different row.
      Bytes.toBytes("val1"), put3);
    System.out.println("Put applied: " + res4); // co CheckAndPutExample-b-SOUT4 We will not get here as an exception is thrown beforehand!
    // ^^ CheckAndPutExample
  }
}
