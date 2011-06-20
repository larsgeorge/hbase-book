package thrift;

// cc ThriftExample Example using the Thrift generated client API
import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftExample {

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
        ByteBuffer.wrap(VALUE)));
      client.mutateRow(ByteBuffer.wrap(TABLE), ByteBuffer.wrap(ROW),
        mutations);

      ArrayList<byte[]> columnNames = new ArrayList<byte[]>();
      columnNames.add(FAMILY2);
      int scannerId = client.scannerOpen(ByteBuffer.wrap(TABLE), null,
        null);
      while (client.scannerGet(scannerId) != null)
        ;
      client.scannerClose(scannerId);
      System.out.println("Done.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
