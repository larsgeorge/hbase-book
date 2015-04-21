package thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import util.HBaseHelper;

// cc ThriftExample Example using the Thrift generated client API
public class ThriftExample {

  // vv ThriftExample
  private static final byte[] TABLE = Bytes.toBytes("testtable");
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final byte[] FAMILY1 = Bytes.toBytes("testFamily1");
  private static final byte[] FAMILY2 = Bytes.toBytes("testFamily2");
  private static final byte[] QUALIFIER = Bytes.toBytes
    ("testQualifier");
  private static final byte[] COLUMN = Bytes.toBytes(
    "testFamily1:testColumn");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  public static void main(String[] args) throws Exception {
    // ^^ ThriftExample
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    // vv ThriftExample
    TTransport transport = new TSocket("0.0.0.0", 9090, 20000);
    TProtocol protocol = new TBinaryProtocol(transport, true, true);
    Hbase.Client client = new Hbase.Client(protocol);
    transport.open();

    ArrayList<ColumnDescriptor> columns = new
      ArrayList<ColumnDescriptor>();
    ColumnDescriptor cd = new ColumnDescriptor();
    cd.name = ByteBuffer.wrap(FAMILY1);
    columns.add(cd);
    cd = new ColumnDescriptor();
    cd.name = ByteBuffer.wrap(FAMILY2);
    columns.add(cd);

    client.createTable(ByteBuffer.wrap(TABLE), columns);

    ArrayList<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, ByteBuffer.wrap(COLUMN),
      ByteBuffer.wrap(VALUE), true));
    client.mutateRow(ByteBuffer.wrap(TABLE), ByteBuffer.wrap(ROW),
      mutations, null);

    ArrayList<byte[]> columnNames = new ArrayList<byte[]>();
    columnNames.add(FAMILY2);
    TScan scan = new TScan();
    int scannerId = client.scannerOpenWithScan(ByteBuffer.wrap(TABLE),
      scan, null);
    for (TRowResult result : client.scannerGet(scannerId)) {
      System.out.println("No. columns: " + result.getColumnsSize());
      for (Map.Entry<ByteBuffer, TCell> column :
        result.getColumns().entrySet()) {
        System.out.println("Column name: " + Bytes.toString(
          column.getKey().array()));
        System.out.println("Column value: " + Bytes.toString(
          column.getValue().getValue()));
      }
    }
    client.scannerClose(scannerId);
    System.out.println("Done.");
    transport.close();
  }
  // ^^ ThriftExample
}
