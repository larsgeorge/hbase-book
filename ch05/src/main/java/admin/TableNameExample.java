package admin;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;

// cc TableNameExample Example how to create a TableName in code
public class TableNameExample {

  // vv TableNameExample
  private static void print(String tablename) {
    print(null, tablename);
  }

  private static void print(String namespace, String tablename) {
    System.out.print("Given Namespace: " + namespace +
      ", Tablename: " + tablename + " -> ");
    try {
      System.out.println(namespace != null ?
        TableName.valueOf(namespace, tablename) :
        TableName.valueOf(tablename));
    } catch (Exception e) {
      System.out.println(e.getClass().getSimpleName() +
        ": " + e.getMessage());
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    print("testtable");
    print("testspace:testtable");
    print("testspace", "testtable");
    print("testspace", "te_st-ta.ble");
    print("", "TestTable-100");
    print("tEsTsPaCe", "te_st-table");

    print("");

    // VALID_NAMESPACE_REGEX = "(?:[a-zA-Z_0-9]+)";
    // VALID_TABLE_QUALIFIER_REGEX = "(?:[a-zA-Z_0-9][a-zA-Z_0-9-.]*)";
    print(".testtable");
    print("te_st-space", "te_st-table");
    print("tEsTsPaCe", "te_st-table@dev");
  }
  // ^^ TableNameExample
}
