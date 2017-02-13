package datatasks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc RenameTableExample An example how to rename a table using the API.
public class RenameTableExample {

  // vv RenameTableExample
  private static void renameTable(Admin admin, TableName oldName, TableName newName)
      throws IOException {
    String snapshotName = "SnapRename-" + System.currentTimeMillis();
    admin.disableTable(oldName);
    admin.snapshot(snapshotName, oldName);
    if (admin.tableExists(newName)) {
      admin.disableTable(newName);
      admin.deleteTable(newName);
    }
    try {
      admin.cloneSnapshot(snapshotName, newName);
      admin.deleteTable(oldName);
    } finally {
      admin.deleteSnapshot(snapshotName);
    }
  }
  // ^^ RenameTableExample

  private static void printFirstValue(Table table) throws IOException {
    System.out.println("Table: " + table.getName());
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    Result res = results.next();
    CellScanner scanner = res.cellScanner();
    if (scanner.advance()) {
      Cell cell = scanner.current();
      System.out.println("Value: " + Bytes.toString(cell.getValueArray(),
        cell.getValueOffset(), cell.getValueLength()));
    } else {
      System.out.println("No data");
    }
  }

  public static void main(String[] args)
    throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 100, 100, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);

    // vv RenameTableExample
    Admin admin = connection.getAdmin();
    TableName name = TableName.valueOf("testtable");

    Table table = connection.getTable(name);
    printFirstValue(table);

    TableName rename = TableName.valueOf("newtesttable");
    renameTable(admin, name, rename);

    Table newTable = connection.getTable(rename);
    printFirstValue(newTable);
    // ^^ RenameTableExample

    table.close();
    newTable.close();
    admin.close();
    connection.close();
  }
}
