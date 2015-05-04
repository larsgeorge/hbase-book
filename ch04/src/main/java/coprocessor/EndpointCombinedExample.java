package coprocessor;

import java.io.IOException;
import java.util.Map;

import coprocessor.generated.RowCounterProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import util.HBaseHelper;

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
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    try {
      admin.split(TableName.valueOf("testtable"), Bytes.toBytes("row3"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    TableName name = TableName.valueOf("testtable");
    Table table = connection.getTable(name);
    // wait for the split to be done
    RegionLocator locator = connection.getRegionLocator(name);
    while (locator.getAllRegionLocations().size() < 2)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    try {
      //vv EndpointCombinedExample
      final RowCounterProtos.CountRequest request =
        RowCounterProtos.CountRequest.getDefaultInstance();
      Map<byte[], /*[*/Pair<Long, Long>/*]*/> results = table.coprocessorService(
        RowCounterProtos.RowCountService.class,
        null, null,
        /*[*/new Batch.Call<RowCounterProtos.RowCountService, Pair<Long, Long>>() {
          public Pair<Long, Long> call(RowCounterProtos.RowCountService counter)
          throws IOException {
            BlockingRpcCallback<RowCounterProtos.CountResponse> rowCallback =
              new BlockingRpcCallback<RowCounterProtos.CountResponse>();
            counter.getRowCount(null, request, rowCallback);

            BlockingRpcCallback<RowCounterProtos.CountResponse> cellCallback =
              new BlockingRpcCallback<RowCounterProtos.CountResponse>();
            counter.getCellCount(null, request, cellCallback);

            RowCounterProtos.CountResponse rowResponse = rowCallback.get();
            Long rowCount = rowResponse.hasCount() ?
              rowResponse.getCount() : 0;

            RowCounterProtos.CountResponse cellResponse = cellCallback.get();
            Long cellCount = cellResponse.hasCount() ?
              cellResponse.getCount() : 0;

            return new Pair<Long, Long>(rowCount, cellCount);/*]*/
          }
        }
      );

      /*[*/long totalRows = 0;
      long totalKeyValues = 0;/*]*/
      for (Map.Entry<byte[], /*[*/Pair<Long, Long>/*]*/> entry : results.entrySet()) {
        /*[*/totalRows += entry.getValue().getFirst().longValue();
        totalKeyValues += entry.getValue().getSecond().longValue();
        System.out.println("Region: " + Bytes.toString(entry.getKey()) +
          ", Count: " + entry.getValue());/*]*/
      }
      /*[*/System.out.println("Total Row Count: " + totalRows);
      System.out.println("Total Cell Count: " + totalKeyValues);/*]*/
      // ^^ EndpointCombinedExample
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
}
