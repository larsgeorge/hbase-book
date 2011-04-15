package client;

// cc IncrementMultipleExample Example incrementing multiple counters in one row
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class IncrementMultipleExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("counters");
    helper.createTable("counters", "daily", "weekly");

    HTable table = new HTable(conf, "counters");

    // vv IncrementMultipleExample
    Increment increment1 = new Increment(Bytes.toBytes("20110101"));

    increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 1);
    increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1); // co IncrementMultipleExample-1-Incr1 Increment the counters with various values.
    increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 10);
    increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 10);

    Result result1 = table.increment(increment1); // co IncrementMultipleExample-2-Incr2 Call the actual increment method with the above counter updates and receive the results.

    for (KeyValue kv : result1.raw()) {
      System.out.println("KV: " + kv +
        " Value: " + Bytes.toLong(kv.getValue())); // co IncrementMultipleExample-3-Dump1 Print the KeyValue and returned counter value.
    }

    Increment increment2 = new Increment(Bytes.toBytes("20110101"));

    increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 5);
    increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1); // co IncrementMultipleExample-4-Incr3 Use positive, negative, and zero increment values to achieve the wanted counter changes.
    increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 0);
    increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), -5);

    Result result2 = table.increment(increment2);

    for (KeyValue kv : result2.raw()) {
      System.out.println("KV: " + kv +
        " Value: " + Bytes.toLong(kv.getValue()));
    }
    // ^^ IncrementMultipleExample
  }
}
