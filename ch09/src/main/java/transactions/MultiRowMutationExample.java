package transactions;

import java.io.IOException;
import java.util.List;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;

import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.*;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

// cc MultiRowMutationExample Use the coprocessor based multi-row mutation call
public class MultiRowMutationExample {

  public static void main(String[] args) throws IOException,
    InterruptedException, ServiceException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    TableName tableName = TableName.valueOf("testtable");

    // vv MultiRowMutationExample
    HTableDescriptor htd = new HTableDescriptor(tableName)
      .addFamily(new HColumnDescriptor("colfam1"))
      .addCoprocessor(MultiRowMutationEndpoint.class.getCanonicalName(), // co MultiRowMutationExample-01-SetCopro Set the coprocessor explicitly for the table.
        null, Coprocessor.PRIORITY_SYSTEM, null)
      .setValue(HTableDescriptor.SPLIT_POLICY,
        KeyPrefixRegionSplitPolicy.class.getName()) // co MultiRowMutationExample-02-SetSplitPolicy Set the supplied split policy.
      .setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY,
        String.valueOf(2)); // co MultiRowMutationExample-03-SetPrefixLen Set the length of the prefix keeping entities together to two.

    // ^^ MultiRowMutationExample
    System.out.println("Creating table...");
    // vv MultiRowMutationExample
    Admin admin = connection.getAdmin();
    admin.createTable(htd);
    Table table = connection.getTable(tableName);

    // ^^ MultiRowMutationExample
    System.out.println("Filling table with test data...");
    // vv MultiRowMutationExample
    for (int i = 0; i < 10; i++) { // co MultiRowMutationExample-04-FillOne Fill first entity prefixed with two zeros, adding 10 rows.
      Put put = new Put(Bytes.toBytes("00-row" + i));
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
        Bytes.toBytes("val1"));
      table.put(put);
    }

    for (int i = 0; i < 10000; i++) { // co MultiRowMutationExample-05-FillTwo Fill second entity prefixed with two nines, adding 10k rows.
      Put put = new Put(Bytes.toBytes("99-row" + i));
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
        Bytes.toBytes("val1"));
      table.put(put);
    }

    // ^^ MultiRowMutationExample
    System.out.println("Flushing table...");
    // vv MultiRowMutationExample
    admin.flush(tableName); // co MultiRowMutationExample-06-Flush Force a flush of the created data.
    Thread.sleep(3 * 1000L);

    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    int numRegions = regions.size();

    // ^^ MultiRowMutationExample
    System.out.println("Number of regions: " + numRegions);
    System.out.println("Splitting table...");
    // vv MultiRowMutationExample
    admin.split(tableName); // co MultiRowMutationExample-07-Split Subsequently split the table to test the split policy.
    do {
      regions = admin.getTableRegions(tableName);
      Thread.sleep(1 * 1000L);
      System.out.print(".");
    } while (regions.size() <= numRegions);
    numRegions = regions.size();
    System.out.println("Number of regions: " + numRegions);
    System.out.println("Regions: ");
    for (HRegionInfo info : regions) { // co MultiRowMutationExample-08-CheckBoundaries The region was split exactly between the two entities, despite the difference in size.
      System.out.print("  Start Key: " + Bytes.toString(info.getStartKey()));
      System.out.println(", End Key: " + Bytes.toString(info.getEndKey()));
    }

    MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();

    Put put = new Put(Bytes.toBytes("00-row1"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val99999"));
    builder.addMutationRequest(ProtobufUtil.toMutation(
      ClientProtos.MutationProto.MutationType.PUT, put)); // co MultiRowMutationExample-09-AddPuts Add puts that address separate rows within the same entity (prefixed with two zeros).
    put = new Put(Bytes.toBytes("00-row5"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val99999"));
    builder.addMutationRequest(ProtobufUtil.toMutation(
    ClientProtos.MutationProto.MutationType.PUT, put));

    // ^^ MultiRowMutationExample
    System.out.println("Calling mutation service...");
    // vv MultiRowMutationExample
    CoprocessorRpcChannel channel = table.coprocessorService(
      Bytes.toBytes("00")); // co MultiRowMutationExample-10-Endpoint Get the endpoint to the region that holds the proper entity (same prefix).
    MultiRowMutationService.BlockingInterface service =
       MultiRowMutationService.newBlockingStub(channel);
    MutateRowsRequest request = builder.build();
    service.mutateRows(null, request); // co MultiRowMutationExample-11-Mutate Call the mutate method that updates the entity across multiple rows atomically.
    // ^^ MultiRowMutationExample

    System.out.println("Scanning first entity...");
    Scan scan = new Scan()
      .setStartRow(Bytes.toBytes("00"))
      .setStopRow(Bytes.toBytes("01"));
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      System.out.print("Result: " + result);
      byte[] val = result.getValue(Bytes.toBytes("colfam1"),
        Bytes.toBytes("qual1"));
      System.out.println(", Value: " + Bytes.toString(val));
    }

    System.out.println(admin.getTableDescriptor(tableName));
    table.close();
    admin.close();
    connection.close();
  }
}
