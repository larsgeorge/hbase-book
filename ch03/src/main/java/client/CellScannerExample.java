package client;

// cc CellScannerExample Shows how to use the cell scanner
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CellScannerExample {

  public static void main(String[] args) throws Exception {
    // vv CellScannerExample
    Put put = new Put(Bytes.toBytes("testrow"));
    put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-1"),
      Bytes.toBytes("val-1"));
    put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-2"),
      Bytes.toBytes("val-2"));
    put.addColumn(Bytes.toBytes("fam-2"), Bytes.toBytes("qual-3"),
      Bytes.toBytes("val-3"));

    CellScanner scanner = put.cellScanner();
    while (scanner.advance()) {
      Cell cell = scanner.current();
      System.out.println("Cell: " + cell);
    }
    // ^^ CellScannerExample
  }
}
