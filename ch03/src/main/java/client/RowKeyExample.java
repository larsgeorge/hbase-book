package client;

// cc RowKeyExample Example row key usage from existing array
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyExample {

  public static void main(String[] args) {
    // vv RowKeyExample
    byte[] data = new byte[100];
    Arrays.fill(data, (byte) '@');
    String username = "johndoe";
    byte[] username_bytes = username.getBytes(Charset.forName("UTF8"));

    System.arraycopy(username_bytes, 0, data, 45, username_bytes.length);
    System.out.println("data length: " + data.length +
      ", data: " + Bytes.toString(data));

    Put put = new Put(data, 45, username_bytes.length);
    System.out.println("Put: " + put);
    // ^^ RowKeyExample
  }
}
