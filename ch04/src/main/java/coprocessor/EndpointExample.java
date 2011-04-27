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

// cc EndpointExample Example using the custom row-count endpoint
// vv EndpointExample
public class EndpointExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // ^^ EndpointExample
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
    // vv EndpointExample
    HTable table = new HTable(conf, "testtable");
    // ^^ EndpointExample
    // wait for the split to be done
    while (table.getRegionsInfo().size() < 2)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    //vv EndpointExample
    try {
      Map<byte[], Long> results = table.coprocessorExec(
        RowCountProtocol.class, // co EndpointExample-1-ClassName Define the protocol interface being invoked.
        null, null, // co EndpointExample-2-Rows Set start and end row key to "null" to count all rows.
        new Batch.Call<RowCountProtocol, Long>() { // co EndpointExample-3-Batch Create an anonymous class to be sent to all region servers.

          @Override
          public Long call(RowCountProtocol counter) throws IOException {
            return counter.getRowCount(); // co EndpointExample-4-Call The call() method is executing the endpoint functions.
          }
        });

      long total = 0;
      for (Map.Entry<byte[], Long> entry : results.entrySet()) { // co EndpointExample-5-Print Iterate over the returned map, containing the result for each region separately.
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
// ^^ EndpointExample
