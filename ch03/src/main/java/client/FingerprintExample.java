package client;

// cc FingerprintExample Shows what the fingerprint and ID of a data class comprises
import java.net.InetAddress;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class FingerprintExample {

  public static void main(String[] args) throws Exception {
    // vv FingerprintExample
    Put put = new Put(Bytes.toBytes("testrow"));
    put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-1"),
      Bytes.toBytes("val-1"));
    put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-2"),
      Bytes.toBytes("val-2"));
    put.addColumn(Bytes.toBytes("fam-2"), Bytes.toBytes("qual-3"),
      Bytes.toBytes("val-3"));

    String id = String.format("Hostname: %s, App: %s",
      InetAddress.getLocalHost().getHostName(),
      System.getProperty("sun.java.command"));
    put.setId(id);

    System.out.println("Put.size: " + put.size());
    System.out.println("Put.id: " + put.getId());
    System.out.println("Put.fingerprint: " + put.getFingerprint());
    System.out.println("Put.toMap: " + put.toMap());
    System.out.println("Put.toJSON: " + put.toJSON());
    System.out.println("Put.toString: " + put.toString());
    // ^^ FingerprintExample
  }
}
