package thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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
    "testQualifier:testColumn");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  public static void main(String[] args) throws IOException {
    try {
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
      int scannerId = client.scannerOpen(ByteBuffer.wrap(TABLE), null,
        null, null);
      for (TRowResult result : client.scannerGet(scannerId)) {
        System.out.println("Result: " + result);
      }
      client.scannerClose(scannerId);
      System.out.println("Done.");
      transport.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  // ^^ ThriftExample
}
