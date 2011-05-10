package client;

// cc RowLockExample Example using row locks explicitly.
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class RowLockExample {

  private final static byte[] ROW1 = Bytes.toBytes("row1");
  private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
  private final static byte[] QUAL1 = Bytes.toBytes("qual1");
  private final static byte[] VAL1 = Bytes.toBytes("val1");
  private final static byte[] VAL2 = Bytes.toBytes("val2");
  private final static byte[] VAL3 = Bytes.toBytes("val3");

  private static Configuration conf = HBaseConfiguration.create();

    // vv RowLockExample
    static class UnlockedPut implements Runnable { // co RowLockExample-1-Thread Use asynchronous thread to update the same row but without a lock.
      @Override
      public void run() {
        try {
          HTable table = new HTable(conf, "testtable");
          Put put = new Put(ROW1);
          put.add(COLFAM1, QUAL1, VAL3);
          long time = System.currentTimeMillis();
          System.out.println("Thread trying to put same row now...");
          table.put(put); // co RowLockExample-2-Block The put() call will block until the lock is released.
          System.out.println("Wait time: " +
            (System.currentTimeMillis() - time) + "ms");
        } catch (IOException e) {
          System.err.println("Thread error: " + e);
        }
      }
    }

    // ^^ RowLockExample

  public static void main(String[] args) throws IOException {
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");

    HTable table = new HTable(conf, "testtable");

    // vv RowLockExample
    System.out.println("Taking out lock...");
    RowLock lock = table.lockRow(ROW1); // co RowLockExample-3-Lock Lock the entire row.
    System.out.println("Lock ID: " + lock.getLockId());

    Thread thread = new Thread(new UnlockedPut()); // co RowLockExample-4-Start Start the asynchronous thread, which will block.
    thread.start();

    try {
      System.out.println("Sleeping 5secs in main()..."); // co RowLockExample-5-Sleep Sleep for some time to block other writers.
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      // ignore
    }

    try {
      Put put1 = new Put(ROW1, lock); // co RowLockExample-6-NewPut1 Create Put under own lock.
      put1.add(COLFAM1, QUAL1, VAL1);
      table.put(put1);

      Put put2 = new Put(ROW1, lock); // co RowLockExample-7-NewPut2 Create another Put under own lock.
      put2.add(COLFAM1, QUAL1, VAL2);
      table.put(put2);
    } catch (Exception e) {
      System.err.println("Error: " + e);
    } finally {
      System.out.println("Releasing lock..."); // co RowLockExample-8-Unlock Release the lock, which will make the thread continue.
      table.unlockRow(lock);
    }
    // ^^ RowLockExample
    try {
      thread.join();
    } catch (InterruptedException e) {
      // ignore
    }
    System.out.println("After thread ended...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);
  }
}
