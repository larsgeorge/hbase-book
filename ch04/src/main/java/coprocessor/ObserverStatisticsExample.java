package coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import coprocessor.generated.ObserverStatisticsProtos;
import coprocessor.generated.ObserverStatisticsProtos.NameInt32Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;
import static coprocessor.generated.ObserverStatisticsProtos.*;

// cc ObserverStatisticsExample Use an endpoint to query observer statistics
public class ObserverStatisticsExample {

  // vv ObserverStatisticsExample
  private static Table table = null;

  private static void printStatistics(boolean print, boolean clear)
  throws Throwable {
    final StatisticsRequest request = StatisticsRequest
      .newBuilder().setClear(clear).build();
    Map<byte[], Map<String, Integer>> results = table.coprocessorService(
      ObserverStatisticsService.class,
      null, null,
      new Batch.Call<ObserverStatisticsProtos.ObserverStatisticsService,
                     Map<String, Integer>>() {
        public Map<String, Integer> call(
          ObserverStatisticsService statistics)
        throws IOException {
          BlockingRpcCallback<StatisticsResponse> rpcCallback =
            new BlockingRpcCallback<StatisticsResponse>();
          statistics.getStatistics(null, request, rpcCallback);
          StatisticsResponse response = rpcCallback.get();
          Map<String, Integer> stats = new LinkedHashMap<String, Integer>();
          for (NameInt32Pair pair : response.getAttributeList()) {
            stats.put(pair.getName(), pair.getValue());
          }
          return stats;
        }
      }
    );
    if (print) {
      for (Map.Entry<byte[], Map<String, Integer>> entry : results.entrySet()) {
        System.out.println("Region: " + Bytes.toString(entry.getKey()));
        for (Map.Entry<String, Integer> call : entry.getValue().entrySet()) {
          System.out.println("  " + call.getKey() + ": " + call.getValue());
        }
      }
      System.out.println();
    }
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    // vv ObserverStatisticsExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 3, "colfam1", "colfam2");
    helper.put("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      new String[]{"colfam1", "colfam2"}, new String[]{"qual1", "qual1"},
      new long[]{1, 2}, new String[]{"val1", "val2"});
    System.out.println("Before endpoint call...");
    helper.dump("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      null, null);
    // vv ObserverStatisticsExample
    try {
      TableName tableName = TableName.valueOf("testtable");
      table = connection.getTable(tableName);
      // ^^ ObserverStatisticsExample
      printStatistics(false, true);
      // vv ObserverStatisticsExample

      System.out.println("Apply single put...");
      Put put = new Put(Bytes.toBytes("row10"));
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
        Bytes.toBytes("val10"));
      table.put(put);
      printStatistics(true, true);

      System.out.println("Do single get...");
      Get get = new Get(Bytes.toBytes("row10"));
      get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"));
      table.get(get);
      printStatistics(true, true);
      /*...*/
      // ^^ ObserverStatisticsExample

      System.out.println("Send batch with put and get...");
      List<Row> batch = new ArrayList<Row>();
      Object[] results = new Object[2];
      batch.add(put);
      batch.add(get);
      table.batch(batch, results);
      printStatistics(true, true);

      System.out.println("Scan single row...");
      Scan scan = new Scan()
        .setStartRow(Bytes.toBytes("row10"))
        .setStopRow(Bytes.toBytes("row11"));
      ResultScanner scanner = table.getScanner(scan);
      System.out.println("  -> after getScanner()...");
      printStatistics(true, true);
      Result result = scanner.next();
      System.out.println("  -> after next()...");
      printStatistics(true, true);
      scanner.close();
      System.out.println("  -> after close()...");
      printStatistics(true, true);

      System.out.println("Scan multiple rows...");
      scan = new Scan();
      scanner = table.getScanner(scan);
      System.out.println("  -> after getScanner()...");
      printStatistics(true, true);
      result = scanner.next();
      System.out.println("  -> after next()...");
      printStatistics(true, true);
      result = scanner.next();
      printStatistics(false, true);
      scanner.close();
      System.out.println("  -> after close()...");
      printStatistics(true, true);

      System.out.println("Apply single put with mutateRow()...");
      RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
        Bytes.toBytes("val10"));
      mutations.add(put);
      table.mutateRow(mutations);
      printStatistics(true, true);

      System.out.println("Apply single column increment...");
      Increment increment = new Increment(Bytes.toBytes("row10"));
      increment.addColumn(Bytes.toBytes("colfam1"),
        Bytes.toBytes("qual11"), 1);
      table.increment(increment);
      printStatistics(true, true);

      System.out.println("Apply multi column increment...");
      increment = new Increment(Bytes.toBytes("row10"));
      increment.addColumn(Bytes.toBytes("colfam1"),
        Bytes.toBytes("qual12"), 1);
      increment.addColumn(Bytes.toBytes("colfam1"),
        Bytes.toBytes("qual13"), 1);
      table.increment(increment);
      printStatistics(true, true);

      System.out.println("Apply single incrementColumnValue...");
      table.incrementColumnValue(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual12"), 1);
      printStatistics(true, true);

      System.out.println("Call single exists()...");
      table.exists(get);
      printStatistics(true, true);

      System.out.println("Apply single delete...");
      Delete delete = new Delete(Bytes.toBytes("row10"));
      delete.addColumn(Bytes.toBytes("colfam1"),
        Bytes.toBytes("qual10"));
      table.delete(delete);
      printStatistics(true, true);

      System.out.println("Apply single append...");
      Append append = new Append(Bytes.toBytes("row10"));
      append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual15"),
        Bytes.toBytes("-valnew"));
      table.append(append);
      printStatistics(true, true);

      System.out.println("Apply checkAndPut (failing)...");
      put = new Put(Bytes.toBytes("row10"));
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual17"),
        Bytes.toBytes("val17"));
      boolean cap = table.checkAndPut(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual15"), null, put);
      System.out.println("  -> success: " + cap);
      printStatistics(true, true);

      System.out.println("Apply checkAndPut (succeeding)...");
      cap = table.checkAndPut(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual16"), null, put);
      System.out.println("  -> success: " + cap);
      printStatistics(true, true);

      System.out.println("Apply checkAndDelete (failing)...");
      delete = new Delete(Bytes.toBytes("row10"));
      delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual17"));
      cap = table.checkAndDelete(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual15"), null, delete);
      System.out.println("  -> success: " + cap);
      printStatistics(true, true);

      System.out.println("Apply checkAndDelete (succeeding)...");
      cap = table.checkAndDelete(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual18"), null, delete);
      System.out.println("  -> success: " + cap);
      printStatistics(true, true);

      System.out.println("Apply checkAndMutate (failing)...");
      mutations = new RowMutations(Bytes.toBytes("row10"));
      put = new Put(Bytes.toBytes("row10"));
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual20"),
        Bytes.toBytes("val20"));
      delete = new Delete(Bytes.toBytes("row10"));
      delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual17"));
      mutations.add(put);
      mutations.add(delete);
      cap = table.checkAndMutate(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
        CompareFilter.CompareOp.GREATER, Bytes.toBytes("val10"), mutations);
      System.out.println("  -> success: " + cap);
      printStatistics(true, true);

      System.out.println("Apply checkAndMutate (succeeding)...");
      cap = table.checkAndMutate(Bytes.toBytes("row10"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
        CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val10"), mutations);
      System.out.println("  -> success: " + cap);
      printStatistics(true, true);
      // vv ObserverStatisticsExample
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
  // ^^ ObserverStatisticsExample
}
