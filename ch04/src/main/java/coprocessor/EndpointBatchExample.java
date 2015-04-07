package coprocessor;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

import coprocessor.generated.RowCounterProtos.CountRequest;
import coprocessor.generated.RowCounterProtos.CountResponse;
import coprocessor.generated.RowCounterProtos.RowCountService;

// cc EndpointBatchExample Example using the custom row-count endpoint in batch mode
public class EndpointBatchExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    // ^^ EndpointBatchExample
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
    Admin admin = connection.getAdmin();
    try {
      admin.split(tableName, Bytes.toBytes("row3"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    // wait for the split to be done
    while (admin.getTableRegions(tableName).size() < 2)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    Table table = connection.getTable(tableName);
    try {
      //vv EndpointBatchExample
      final CountRequest request = CountRequest.getDefaultInstance();
      Map<byte[], CountResponse> results = /*[*/table.batchCoprocessorService(
        RowCountService.getDescriptor().findMethodByName("getRowCount"),
        request, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
        CountResponse.getDefaultInstance());/*]*/

      long total = 0;
      for (Map.Entry<byte[], /*[*/CountResponse/*]*/> entry : results.entrySet()) {
        /*[*/CountResponse response = entry.getValue();/*]*/
        total += /*[*/response.hasCount() ? response.getCount() : 0;/*]*/
        System.out.println("Region: " + Bytes.toString(entry.getKey()) +
          ", Count: " + entry.getValue());
      }
      System.out.println("Total Count: " + total);
      // ^^ EndpointBatchExample
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
}
