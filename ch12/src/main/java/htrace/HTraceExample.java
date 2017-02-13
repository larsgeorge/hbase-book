package htrace;

import java.io.IOException;

import org.apache.htrace.Sampler;
import org.apache.htrace.SamplerBuilder;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.impl.ProbabilitySampler;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.trace.HBaseHTraceConfiguration;
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
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc HTraceExample Shows the use of the HBase HTrace integration
public class HTraceExample {

  // vv HTraceExample
  private static SpanReceiverHost spanReceiverHost;
  // ^^ HTraceExample

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 100, 100, "colfam1");

    // vv HTraceExample
    conf.set("hbase.trace.spanreceiver.classes",
      "org.apache.htrace.impl.ZipkinSpanReceiver"); // co HTraceExample-1-Conf Set up configuration to use the Zipkin span receiver class.
    conf.set("hbase.htrace.zipkin.collector-hostname", "localhost");
    conf.set("hbase.htrace.zipkin.collector-port", "9410");

    spanReceiverHost = SpanReceiverHost.getInstance(conf); // co HTraceExample-2-GetInstance Initialize the span receiver host from the configuration settings.
    // ^^ HTraceExample

    Connection connection = null;
    TraceScope ts0 = Trace.startSpan("Connection Trace", Sampler.ALWAYS);
    try {
      connection = ConnectionFactory.createConnection(conf);
    } finally {
      ts0.close();
    }

    Admin admin = connection.getAdmin();
    admin.flush(TableName.valueOf("testtable"));
    Thread.sleep(3000);

    // vv HTraceExample
    Table table = connection.getTable(TableName.valueOf("testtable"));

    TraceScope ts1 = Trace.startSpan("Get Trace", Sampler.ALWAYS); // co HTraceExample-2-Start Start a span, giving it a name and sample rate.
    try {
      Get get = new Get(Bytes.toBytes("row-1")); // co HTraceExample-3-Default Perform common operations that should be traced.
      Result res = table.get(get);
    } finally {
      ts1.close(); // co HTraceExample-4-Close Close the span to group performance details together.
    }
    System.out.println("Is trace detached? " + ts1.isDetached()); // co HTraceExample-5-Span Talk to the trace and span instances from within the code.
    Span span = ts1.getSpan();
    System.out.println("Span Time: " + span.getAccumulatedMillis());
    System.out.println("Span: " + span);

    //conf.set("hbase.htrace.sampler", "ProbabilitySampler");
    //conf.set("hbase.htrace.sampler.fraction", "0.5");
    conf.set("hbase.htrace.sampler", "CountSampler");
    conf.set("hbase.htrace.sampler.frequency", "5");
    HBaseHTraceConfiguration traceConf = new HBaseHTraceConfiguration(conf);
    SamplerBuilder builder = new SamplerBuilder(traceConf);
    Sampler sampler = builder.build();
    System.out.println("Sampler: " + sampler.getClass().getName());

    TraceScope ts2 = Trace.startSpan("Scan Trace", sampler); // co HTraceExample-6-Scan Start another span with a different sampler.
    try {
      Scan scan = new Scan();
      scan.setCaching(1); // co HTraceExample-7-OneRow The scan performs a separate RPC call for each row it retrieves, creating a span for every row.
      ResultScanner scanner = table.getScanner(scan);
      while (scanner.next() != null) ;
      scanner.close();
    } finally {
      ts2.close();
    }
    // ^^ HTraceExample
    table.close();
    connection.close();
    admin.close();
  }
}
