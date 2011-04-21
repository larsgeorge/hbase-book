package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;
import java.util.Map;

// cc EndpointForMethodExample Example of how the Batch.forMethod() can reduce the client code size
public class EndpointForMethodExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.put("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      new String[]{"colfam1", "colfam2"},
      new String[]{"qual1", "qual1"},
      new long[]{1, 2},
      new String[]{"val1", "val2"});
    System.out.println("Before endpoint call...");
    helper.dump("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      null, null);
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.split("testtable", "row3");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    HTable table = new HTable(conf, "testtable");
    while (table.getRegionsInfo().size() < 2)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    try {
      //vv EndpointForMethodExample
      /*[*/
      Batch.Call call = Batch.forMethod(RowCountProtocol.class,
        "getKeyValueCount");/*]*/
      Map<byte[], Long> results = table.coprocessorExec(
        RowCountProtocol.class, null, null, call);
      // ^^ EndpointForMethodExample

      long total = 0;
      for (Map.Entry<byte[], Long> entry : results.entrySet()) {
        total += entry.getValue().longValue();
        System.out.println("Region: " + Bytes.toString(entry.getKey()) +
          ", Count: " + entry.getValue());
      }
      System.out.println("Total Count: " + total);
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
}
