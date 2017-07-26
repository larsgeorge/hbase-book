package thrift;

import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;

import util.HBaseHelper;

// cc SecureThriftExample Example using the Thrift generated client API
public class SecureThriftExample {

  // vv SecureThriftExample
  private static final byte[] TABLE = Bytes.toBytes("testtable");
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final byte[] FAMILY1 = Bytes.toBytes("testFamily1");
  private static final byte[] FAMILY2 = Bytes.toBytes("testFamily2");
  private static final byte[] QUALIFIER = Bytes.toBytes
    ("testQualifier");
  private static final byte[] COLUMN = Bytes.toBytes(
    "testFamily1:testColumn");
  private static final byte[] COLUMN2 = Bytes.toBytes(
    "testFamily2:testColumn2");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  static String hostname, principal;
  static int port;

  public static void main(String[] args) throws Exception {
    hostname = args.length > 0 ? args[0] : "master-2.hbase.book";
    port = args.length >= 2 ? Integer.parseInt(args[1]) : 9090;
    principal = args.length >= 3 ? args[2] : "hbase-thrift";

    final SecureThriftExample client = new SecureThriftExample();
    Subject.doAs(getSubject(), new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        client.run();
        return null;
      }
    });
  }

  private void run() throws Exception {
    // ^^ SecureThriftExample
    Configuration conf = HBaseConfiguration.create();
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    // vv SecureThriftExample
    System.out.println("Using port " + port + " and server " + hostname);
    TTransport transport = new TSocket(hostname, port);
    Map<String, String> saslProperties = new HashMap<>();
    saslProperties.put(Sasl.QOP, "auth-conf,auth-int,auth");
    TTransport saslTransport = new TSaslClientTransport(
      "GSSAPI", null,
      principal, // Thrift server user name, should be an authorized proxy user.
      hostname, // Thrift server domain
      saslProperties, null, transport);

    TProtocol protocol = new TBinaryProtocol(saslTransport, true, true);
    Hbase.Client client = new Hbase.Client(protocol);

    saslTransport.open();

    ArrayList<ColumnDescriptor> columns = new
      ArrayList<ColumnDescriptor>();
    ColumnDescriptor cd = new ColumnDescriptor();
    cd.name = ByteBuffer.wrap(FAMILY1);
    columns.add(cd);
    cd = new ColumnDescriptor();
    cd.name = ByteBuffer.wrap(FAMILY2);
    columns.add(cd);

    client.createTable(ByteBuffer.wrap(TABLE), columns);

    ArrayList<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, ByteBuffer.wrap(COLUMN),
      ByteBuffer.wrap(VALUE), true));
    mutations.add(new Mutation(false, ByteBuffer.wrap(COLUMN2),
      ByteBuffer.wrap(VALUE), true));
    client.mutateRow(ByteBuffer.wrap(TABLE), ByteBuffer.wrap(ROW),
      mutations, null);

    TScan scan = new TScan();
    int scannerId = client.scannerOpenWithScan(ByteBuffer.wrap(TABLE),
      scan, null);
    for (TRowResult result : client.scannerGet(scannerId)) {
      System.out.println("No. columns: " + result.getColumnsSize());
      for (Map.Entry<ByteBuffer, TCell> column :
        result.getColumns().entrySet()) {
        System.out.println("Column name: " + Bytes.toString(
          column.getKey().array()));
        System.out.println("Column value: " + Bytes.toString(
          column.getValue().getValue()));
      }
    }
    client.scannerClose(scannerId);

    ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
    columnNames.add(ByteBuffer.wrap(FAMILY1));
    scannerId = client.scannerOpen(ByteBuffer.wrap(TABLE),
      ByteBuffer.wrap(Bytes.toBytes("")), columnNames, null);
    for (TRowResult result : client.scannerGet(scannerId)) {
      System.out.println("No. columns: " + result.getColumnsSize());
      for (Map.Entry<ByteBuffer, TCell> column :
        result.getColumns().entrySet()) {
        System.out.println("Column name: " + Bytes.toString(
          column.getKey().array()));
        System.out.println("Column value: " + Bytes.toString(
          column.getValue().getValue()));
      }
    }
    client.scannerClose(scannerId);

    System.out.println("Done.");
    saslTransport.close();
  }
  // ^^ SecureThriftExample

  static Subject getSubject() throws Exception {
    /*
     * To authenticate the DemoClient, kinit should be invoked ahead.
     * Here we try to get the Kerberos credential from the ticket cache.
     */
    LoginContext context = new LoginContext("", new Subject(), null,
      new javax.security.auth.login.Configuration() {
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
          Map<String, String> options = new HashMap<>();
          options.put("useKeyTab", "false");
          options.put("storeKey", "false");
          options.put("doNotPrompt", "true");
          options.put("useTicketCache", "true");
          options.put("renewTGT", "true");
          options.put("refreshKrb5Config", "true");
          options.put("isInitiator", "true");
          String ticketCache = System.getenv("KRB5CCNAME");
          if (ticketCache != null) {
            options.put("ticketCache", ticketCache);
          }
          options.put("debug", "true");

          return new AppConfigurationEntry[]{
              new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  options)};
        }
      });
    context.login();
    return context.getSubject();
  }
}
