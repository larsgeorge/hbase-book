package client;

// cc GetRowOrBeforeExample Example using a special retrieval method.
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class GetRowOrBeforeExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    if (!helper.existsTable("testtable")) {
      helper.createTable("testtable", "colfam1");
    }
    HTable table = new HTable(conf, "testtable");

    // vv GetRowOrBeforeExample
    Result result1 = table.getRowOrBefore(Bytes.toBytes("row1"), // co GetRowOrBeforeExample-1-GetRow1 Attempt to find an existing row.
      Bytes.toBytes("colfam1"));
    System.out.println("Found: " + Bytes.toString(result1.getRow())); // co GetRowOrBeforeExample-2-SOUT1 Print what was found.

    Result result2 = table.getRowOrBefore(Bytes.toBytes("row99"), // co GetRowOrBeforeExample-3-GetRow2 Attempt to find a non-existent row.
      Bytes.toBytes("colfam1"));
    System.out.println("Found: " + Bytes.toString(result2.getRow())); // co GetRowOrBeforeExample-4-SOUT2 Returns the row that was sorted at the end of the table.

    for (KeyValue kv : result2.raw()) {
      System.out.println("  Col: " + Bytes.toString(kv.getFamily()) + // co GetRowOrBeforeExample-5-Dump Print the returned values.
        "/" + Bytes.toString(kv.getQualifier()) +
        ", Value: " + Bytes.toString(kv.getValue()));
    }

    Result result3 = table.getRowOrBefore(Bytes.toBytes("abc"), // co GetRowOrBeforeExample-6-GetRow3 Attempt to find a row before the test rows.
      Bytes.toBytes("colfam1"));
    System.out.println("Found: " + result3); // co GetRowOrBeforeExample-7-SOUT3 Should return "null" since there is no match.
    // ^^ GetRowOrBeforeExample
  }
}
