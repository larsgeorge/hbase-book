package admin;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc SnapshotExample Example showing the use of the admin snapshot API
public class SnapshotExample {

  public static void main(String[] args)
    throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.dropTable("testtable2");
    helper.dropTable("testtable3");
    helper.createTable("testtable", 3, "colfam1", "colfam2");
    helper.put("testtable", new String[]{"row1"},
      new String[]{"colfam1", "colfam2"},
      new String[]{"qual1", "qual1", "qual2", "qual2", "qual3", "qual3"},
      new long[]{1, 2, 3, 4, 5, 6},
      new String[]{"val1", "val1", "val2", "val2", "val3", "val3"});
    System.out.println("Before snapshot calls...");
    helper.dump("testtable", new String[]{"row1"}, null, null);

    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf("testtable");
    Table table = connection.getTable(tableName);
    Admin admin = connection.getAdmin();

    /*
    When you try to snapshot with an existing name:
    Exception in thread "main" org.apache.hadoop.hbase.client.RetriesExhaustedException:
      Failed after attempts=1, exceptions:
    Mon Apr 13 11:21:58 CEST 2015, RpcRetryingCaller{globalStartTime=1428916918532, pause=100, retries=1}, org.apache.hadoop.hbase.ipc.RemoteWithExtrasException(org.apache.hadoop.hbase.snapshot.SnapshotExistsException): org.apache.hadoop.hbase.snapshot.SnapshotExistsException: Snapshot 'snapshot2' already stored on the filesystem.
    	at org.apache.hadoop.hbase.master.snapshot.SnapshotManager.takeSnapshot(SnapshotManager.java:518)
      ...
     */
    admin.deleteSnapshots("snapshot.*");

    // vv SnapshotExample
    admin.snapshot("snapshot1", tableName); // co SnapshotExample-1-Snap1 Create a snapshot of the initial table, then list all available snapshots next.

    List<HBaseProtos.SnapshotDescription> snaps = admin.listSnapshots();
    System.out.println("Snapshots after snapshot 1: " + snaps);

    Delete delete = new Delete(Bytes.toBytes("row1"));
    delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co SnapshotExample-2-Delete Remove one column and do two more snapshots, one without first flushing, then another with a preceding flush.
    table.delete(delete);

    admin.snapshot("snapshot2", tableName,
      HBaseProtos.SnapshotDescription.Type.SKIPFLUSH);
    admin.snapshot("snapshot3", tableName,
      HBaseProtos.SnapshotDescription.Type.FLUSH);

    snaps = admin.listSnapshots();
    System.out.println("Snapshots after snapshot 2 & 3: " + snaps);

    Put put = new Put(Bytes.toBytes("row2"))
      .addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual10"),
        // co SnapshotExample-3-Put Add a new row to the table and take yet another snapshot.
        Bytes.toBytes("val10"));
    table.put(put);

    HBaseProtos.SnapshotDescription snapshotDescription =
      HBaseProtos.SnapshotDescription.newBuilder()
      .setName("snapshot4")
      .setTable(tableName.getNameAsString())
      .build();
    admin.takeSnapshotAsync(snapshotDescription);

    snaps = admin.listSnapshots();
    System.out.println("Snapshots before waiting: " + snaps);

    System.out.println("Waiting...");
    while (!admin.isSnapshotFinished(snapshotDescription)) { // co SnapshotExample-4-Wait Wait for the asynchronous snapshot to complete. List the snapshots before and after the waiting.
      Thread.sleep(1 * 1000);
      System.out.print(".");
    }
    System.out.println();
    System.out.println("Snapshot completed.");
    snaps = admin.listSnapshots();
    System.out.println("Snapshots after waiting: " + snaps);

    System.out.println("Table before restoring snapshot 1");
    helper.dump("testtable", new String[]{"row1", "row2"}, null, null);

    // ^^ SnapshotExample
    /*
    If the table is not disabled you will receive this error:
    Exception in thread "main" org.apache.hadoop.hbase.TableNotDisabledException: testtable
    	at org.apache.hadoop.hbase.client.HBaseAdmin.restoreSnapshot(HBaseAdmin.java:3153)
    	at org.apache.hadoop.hbase.client.HBaseAdmin.restoreSnapshot(HBaseAdmin.java:3088)
    	at admin.SnapshotExample.main(SnapshotExample.java:88)
      ...
    	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:140)
     */
    // vv SnapshotExample
    admin.disableTable(tableName);
    admin.restoreSnapshot("snapshot1"); // co SnapshotExample-5-Restore Restore the first snapshot, recreating the initial table. This needs to be done on a disabled table.
    admin.enableTable(tableName);

    System.out.println("Table after restoring snapshot 1");
    helper.dump("testtable", new String[]{"row1", "row2"}, null, null);

    admin.deleteSnapshot("snapshot1"); // co SnapshotExample-6-DelSnap1 Remove the first snapshot, and list the available ones again.
    snaps = admin.listSnapshots();
    System.out.println("Snapshots after deletion: " + snaps);

    admin.cloneSnapshot("snapshot2", TableName.valueOf("testtable2"));
    System.out.println("New table after cloning snapshot 2");
    helper.dump("testtable2", new String[]{"row1", "row2"}, null, null);
    admin.cloneSnapshot("snapshot3", TableName.valueOf("testtable3")); // co SnapshotExample-7-Clone Clone the second and third snapshot into a new table, dump the content to show the difference between the "skipflush" and "flush" types.
    System.out.println("New table after cloning snapshot 3");
    helper.dump("testtable3", new String[]{"row1", "row2"}, null, null);

    // ^^ SnapshotExample
    table.close();
    connection.close();
    helper.close();
  }
}
