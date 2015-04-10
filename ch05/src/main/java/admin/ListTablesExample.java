package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import util.HBaseHelper;

// cc ListTablesExample Example listing the existing tables and their descriptors
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
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    HTableDescriptor[] htds = admin.listTables();
    // ^^ ListTablesExample
    System.out.println("Printing all tables...");
    // vv ListTablesExample
    for (HTableDescriptor htd : htds) {
      System.out.println(htd);
    }

    HTableDescriptor htd1 = admin.getTableDescriptor(
      TableName.valueOf("testtable1"));
    // ^^ ListTablesExample
    System.out.println("Printing testtable1...");
    // vv ListTablesExample
    System.out.println(htd1);

    HTableDescriptor htd2 = admin.getTableDescriptor(
      TableName.valueOf("testtable10"));
    // ^^ ListTablesExample
    System.out.println("Printing testtable10...");
    // vv ListTablesExample
    System.out.println(htd2);
    // ^^ ListTablesExample
  }
}
