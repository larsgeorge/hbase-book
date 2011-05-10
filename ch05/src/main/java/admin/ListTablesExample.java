package admin;

// cc ListTablesExample Example listing the existing tables and their descriptors
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class ListTablesExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable1");
    helper.dropTable("testtable2");
    helper.dropTable("testtable3");
    helper.createTable("testtable1", "colfam1", "colfam2", "colfam3");
    helper.createTable("testtable2", "colfam1", "colfam2", "colfam3");
    helper.createTable("testtable3", "colfam1", "colfam2", "colfam3");

    // vv ListTablesExample
    HBaseAdmin admin = new HBaseAdmin(conf);

    HTableDescriptor[] htds = admin.listTables();
    // ^^ ListTablesExample
    System.out.println("Printing all tables...");
    // vv ListTablesExample
    for (HTableDescriptor htd : htds) {
      System.out.println(htd);
    }

    HTableDescriptor htd1 = admin.getTableDescriptor(
      Bytes.toBytes("testtable1"));
    // ^^ ListTablesExample
    System.out.println("Printing testtable1...");
    // vv ListTablesExample
    System.out.println(htd1);

    HTableDescriptor htd2 = admin.getTableDescriptor(
      Bytes.toBytes("testtable10"));
    // ^^ ListTablesExample
    System.out.println("Printing testtable10...");
    // vv ListTablesExample
    System.out.println(htd2);
    // ^^ ListTablesExample
  }
}
