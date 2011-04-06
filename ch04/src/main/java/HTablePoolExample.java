// cc HTablePoolExample Example using the HTablePool class to share HTable instances
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HTablePoolExample {

  public static void main(String[] args) throws IOException {
    // vv HTablePoolExample
    Configuration conf = HBaseConfiguration.create();
    // ^^ HTablePoolExample

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    // vv HTablePoolExample
    HTablePool pool = new HTablePool(conf, 5); // co HTablePoolExample-1-Create Create the pool, allowing 5 HTable's to be retained.

    // ^^ HTablePoolExample
    System.out.println("Acquiring tables...");
    // vv HTablePoolExample
    HTableInterface[] tables = new HTableInterface[10];
    for (int n = 0; n < 10; n++) {
      tables[n] = pool.getTable("testtable"); // co HTablePoolExample-2-GetTable Get 10 HTable references, which is more than the pool is retaining.
      System.out.println(Bytes.toString(tables[n].getTableName()));
    }

    // ^^ HTablePoolExample
    System.out.println("Releasing tables...");
    // vv HTablePoolExample
    for (int n = 0; n < 5; n++) {
      pool.putTable(tables[n]); // co HTablePoolExample-3-PuTable Return HTable instances to the pool, 5 will be kept, while the additional 5 will be dropped.
    }

    // ^^ HTablePoolExample
    System.out.println("Closing pool...");
    // vv HTablePoolExample
    pool.closeTablePool("testtable"); // co HTablePoolExample-4-Close Close the entire pool, releasing all retained table references.
    // ^^ HTablePoolExample
  }
}
