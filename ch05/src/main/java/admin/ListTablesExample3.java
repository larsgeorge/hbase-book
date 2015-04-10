package admin;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import util.HBaseHelper;

// cc ListTablesExample3 Example listing the existing tables with patterns
public class ListTablesExample3 {

  private static void print(TableName[] tableNames) {
    for (TableName name : tableNames) {
      System.out.println(name);
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
    // vv ListTablesExample3
    TableName[] names = admin.listTableNames(".*");
    // ^^ ListTablesExample3
    print(names);
    System.out.println("List: .*, including system tables");
    // vv ListTablesExample3
    names = admin.listTableNames(".*", true);
    // ^^ ListTablesExample3
    print(names);

    System.out.println("List: hbase:.*, including system tables");
    // vv ListTablesExample3
    names = admin.listTableNames("hbase:.*", true);
    // ^^ ListTablesExample3
    print(names);

    System.out.println("List: def.*:.*, including system tables");
    // vv ListTablesExample3
    names = admin.listTableNames("def.*:.*", true);
    // ^^ ListTablesExample3
    print(names);

    System.out.println("List: test.*");
    // vv ListTablesExample3
    names = admin.listTableNames("test.*");
    // ^^ ListTablesExample3
    print(names);

    System.out.println("List: .*2, using Pattern");
    // vv ListTablesExample3
    Pattern pattern = Pattern.compile(".*2");
    names = admin.listTableNames(pattern);
    // ^^ ListTablesExample3
    print(names);

    System.out.println("List by Namespace: testspace1");
    // vv ListTablesExample3
    names = admin.listTableNamesByNamespace("testspace1");
    // ^^ ListTablesExample3
    print(names);
  }
}
