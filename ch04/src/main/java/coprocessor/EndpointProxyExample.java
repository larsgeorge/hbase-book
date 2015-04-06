package coprocessor;

import java.io.IOException;

import coprocessor.generated.RowCounterProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

import static coprocessor.generated.RowCounterProtos.*;

// cc EndpointProxyExample Example using the proxy call of HTable to invoke an endpoint on a single region
public class EndpointProxyExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 3, "colfam1", "colfam2");
    helper.put("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      new String[]{"colfam1", "colfam2"}, new String[]{"qual1", "qual1"},
      new long[]{1, 2}, new String[]{"val1", "val2"});
    System.out.println("Before endpoint call...");
    helper.dump("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"}, null, null);
    Admin admin = connection.getAdmin();
    try {
      admin.split(tableName, Bytes.toBytes("row3"));
    } catch (Exception e) {
      e.printStackTrace();
    }
    Table table = connection.getTable(tableName);
    // wait for the split to be done
    while (admin.getTableRegions(tableName).size() < 2)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    try {
      //vv EndpointProxyExample
      HRegionInfo hri = admin.getTableRegions(tableName).get(0);
      Scan scan = new Scan(hri.getStartKey(), hri.getEndKey())
        .setMaxVersions();
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        System.out.println("Result: " + result);
      }

      CoprocessorRpcChannel channel = table.coprocessorService(
        Bytes.toBytes("row1"));
      RowCountService.BlockingInterface service =
        RowCountService.newBlockingStub(channel);
      CountRequest request = CountRequest.newBuilder().build();
      CountResponse response = service.getCellCount(null, request);
      long cellsInRegion = response.hasCount() ? response.getCount() : -1;
      System.out.println("Region Cell Count: " + cellsInRegion);

      request = CountRequest.newBuilder().build();
      response = service.getRowCount(null, request);
      long rowsInRegion = response.hasCount() ? response.getCount() : -1;
      System.out.println("Region Row Count: " + rowsInRegion);
      // ^^ EndpointProxyExample
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
}
