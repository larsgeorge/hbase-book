package admin;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory;
import org.apache.hadoop.hbase.quotas.QuotaType;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import util.HBaseHelper;

// cc QuotaExample Example using the quota related classes and API
public class QuotaExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropNamespace("bar", true);
    helper.dropNamespace("foo", true);
    helper.createNamespace("bar");
    helper.createNamespace("foo");
    helper.createTable("foo:unlimited", "cf1");
    helper.createTable("foo:limited", "cf1");
    helper.createTable("bar:limited", "cf1");
    System.out.println("Adding rows to tables...");
    helper.fillTable("foo:limited", 1, 10, 1, "cf1");

    // vv QuotaExample
    Connection connection = ConnectionFactory.createConnection(conf);
    TableName fooLimited = TableName.valueOf("foo:limited");
    TableName fooUnlimited = TableName.valueOf("foo:unlimited"); // co QuotaExample-1-Names Create the table name instances for the test tables.
    TableName barLimited = TableName.valueOf("bar:limited");

    Table table = connection.getTable(fooLimited); // co QuotaExample-2-Tables Create a reference to the table and the admin API.
    Admin admin = connection.getAdmin();

    QuotaSettings qs = QuotaSettingsFactory.throttleTable(fooLimited, // co QuotaExample-3-Quota1 Configure a quota setting record at the table level, and assign it.
      ThrottleType.READ_NUMBER, 5, TimeUnit.DAYS);
    admin.setQuota(qs);

    Scan scan = new Scan();
    scan.setCaching(1);
    ResultScanner scanner = table.getScanner(scan); // co QuotaExample-4-Scan Scan the table to measure the effect of the quota.
    int numRows = 0;
    try {
      for (Result res : scanner) {
        System.out.println(res);
        numRows++;
      }
    } catch (Exception e) {
      System.out.println("Error occurred: " + e.getMessage());
    }
    System.out.printf("Number of rows: " + numRows);
    scanner.close();

    qs = QuotaSettingsFactory.throttleUser("hbasebook", "bar", // co QuotaExample-5-Quota2 Configure another quota settings record, this time on the namespace level, and assign it.
      ThrottleType.REQUEST_NUMBER, 5, TimeUnit.SECONDS);
    admin.setQuota(qs);

    QuotaFilter qf = new QuotaFilter();
    // ^^ QuotaExample
    qf.addTypeFilter(QuotaType.THROTTLE);
    //qf.setNamespaceFilter("foo");
    // vv QuotaExample
    QuotaRetriever qr = admin.getQuotaRetriever(qf); // co QuotaExample-6-GetQuotas Configure a filter, get a retriever instance and print out the results.
    System.out.println("Quotas:");
    for (QuotaSettings setting : qr) {
      System.out.println("  Quota Setting: " + setting);
    }
    // ^^ QuotaExample

    table.close();
    admin.close();
    helper.close();
  }
}