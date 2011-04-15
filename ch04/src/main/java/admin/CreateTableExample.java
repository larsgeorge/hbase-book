package admin;

// cc CreateTableExample Example using the administrative API to create a table
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class CreateTableExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    // vv CreateTableExample
    Configuration conf = HBaseConfiguration.create();
    // ^^ CreateTableExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv CreateTableExample

    HBaseAdmin admin = new HBaseAdmin(conf); // co CreateTableExample-1-CreateAdmin Create a administrative API instance.

    HTableDescriptor desc = new HTableDescriptor( // co CreateTableExample-2-CreateHTD Create the table descriptor instance.
      Bytes.toBytes("testtable"));

    HColumnDescriptor coldef = new HColumnDescriptor( // co CreateTableExample-3-CreateHCD Create a column family descriptor and add it to the table descriptor.
      Bytes.toBytes("colfam1"));
    desc.addFamily(coldef);

    admin.createTable(desc); // co CreateTableExample-4-CreateTable Call the createTable() method to do the actual work.

    boolean avail = admin.isTableAvailable(Bytes.toBytes("testtable")); // co CreateTableExample-5-Check Check if the table is available.
    System.out.println("Table available: " + avail);
    // ^^ CreateTableExample
  }
}
