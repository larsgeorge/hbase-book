package admin;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import util.HBaseHelper;

// cc ListTablesExample2 Example listing the existing tables with patterns
public class ListTablesExample2 {

  private static void print(HTableDescriptor[] descriptors) {
    for (HTableDescriptor htd : descriptors) {
      System.out.println(htd.getTableName());
    }
    System.out.println();
  }

  public static void main(String[] args)
  throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropNamespace("testspace1", true);
    helper.dropNamespace("testspace2", true);
    helper.dropTable("testtable3");
    helper.createNamespace("testspace1");
    helper.createNamespace("testspace2");
    helper.createTable("testspace1:testtable1", "colfam1");
    helper.createTable("testspace2:testtable2", "colfam1");
    helper.createTable("testtable3", "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    System.out.println("List: .*");
    // vv ListTablesExample2
    HTableDescriptor[] htds = admin.listTables(".*");
    // ^^ ListTablesExample2
    print(htds);
    System.out.println("List: .*, including system tables");
    // vv ListTablesExample2
    htds = admin.listTables(".*", true);
    // ^^ ListTablesExample2
    print(htds);

    System.out.println("List: hbase:.*, including system tables");
    // vv ListTablesExample2
    htds = admin.listTables("hbase:.*", true);
    // ^^ ListTablesExample2
    print(htds);

    System.out.println("List: def.*:.*, including system tables");
    // vv ListTablesExample2
    htds = admin.listTables("def.*:.*", true);
    // ^^ ListTablesExample2
    print(htds);

    System.out.println("List: test.*");
    // vv ListTablesExample2
    htds = admin.listTables("test.*");
    // ^^ ListTablesExample2
    print(htds);

    System.out.println("List: .*2, using Pattern");
    // vv ListTablesExample2
    Pattern pattern = Pattern.compile(".*2");
    htds = admin.listTables(pattern);
    // ^^ ListTablesExample2
    print(htds);

    System.out.println("List by Namespace: testspace1");
    // vv ListTablesExample2
    htds = admin.listTableDescriptorsByNamespace("testspace1");
    // ^^ ListTablesExample2
    print(htds);
  }
}
