package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.HBaseHelper;

import java.io.IOException;
import java.util.Map;

// cc EndpointCombinedExample Example extending the batch call to execute multiple endpoint calls
public class EndpointCombinedExample {

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
    // wait for the split to be done
    while (table.getRegionsInfo().size() < 2)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    try {
      //vv EndpointCombinedExample
      Map<byte[], Pair<Long, Long>> results = table.coprocessorExec(
        RowCountProtocol.class,
        null, null,
        /*[*/new Batch.Call<RowCountProtocol, Pair<Long, Long>>() {
          public Pair<Long, Long> call(RowCountProtocol counter)
            throws IOException {
            return new Pair(counter.getRowCount(),
              counter.getKeyValueCount());/*]*/
          }
        });

      long totalRows = 0;
      long totalKeyValues = 0;
      for (Map.Entry<byte[], Pair<Long, Long>> entry : results.entrySet()) {
        totalRows += entry.getValue().getFirst().longValue();
        totalKeyValues += entry.getValue().getSecond().longValue();
        System.out.println("Region: " + Bytes.toString(entry.getKey()) +
          ", Count: " + entry.getValue());
      }
      System.out.println("Total Row Count: " + totalRows);
      System.out.println("Total KeyValue Count: " + totalKeyValues);
      // ^^ EndpointCombinedExample
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
}
