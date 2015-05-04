package mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

// cc ParseJsonMulti MapReduce job that parses the raw data into separate tables.
public class ParseJsonMulti {

  private static final Log LOG = LogFactory.getLog(ParseJsonMulti.class);

  public static final String NAME = "ParseJsonMulti";
  public enum Counters { ROWS, COLS, ERROR, VALID }

  /**
   * Implements the <code>Mapper</code> that reads the data and extracts the
   * required information.
   */
  // vv ParseJsonMulti
  static class ParseMapper
  extends TableMapper<ImmutableBytesWritable, Writable> {

    private Connection connection = null;
    private BufferedMutator infoTable = null;
    private BufferedMutator linkTable = null;
    private JSONParser parser = new JSONParser();
    private byte[] columnFamily = null;

    @Override
    protected void setup(Context context)
    throws IOException, InterruptedException {
      connection = ConnectionFactory.createConnection(
        context.getConfiguration());
      infoTable = connection.getBufferedMutator(TableName.valueOf(
        context.getConfiguration().get("conf.infotable"))); // co ParseJsonMulti-1-Setup Create and configure both target tables in the setup() method.
      linkTable = connection.getBufferedMutator(TableName.valueOf(
        context.getConfiguration().get("conf.linktable")));
      columnFamily = Bytes.toBytes(
        context.getConfiguration().get("conf.columnfamily"));
    }

    @Override
    protected void cleanup(Context context)
    throws IOException, InterruptedException {
      infoTable.flush();
      linkTable.flush(); // co ParseJsonMulti-2-Cleanup Flush all pending commits when the task is complete.
    }

    // ^^ ParseJsonMulti
    /**
     * Maps the input.
     *
     * @param row The row key.
     * @param columns The columns of the row.
     * @param context The task context.
     * @throws java.io.IOException When mapping the input fails.
     */
    // vv ParseJsonMulti
    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context)
    throws IOException {
      context.getCounter(Counters.ROWS).increment(1);
      String value = null;
      try {
        Put infoPut = new Put(row.get());
        Put linkPut = new Put(row.get());
        for (Cell cell : columns.listCells()) {
          context.getCounter(Counters.COLS).increment(1);
          value = Bytes.toStringBinary(cell.getValueArray(),
            cell.getValueOffset(), cell.getValueLength());
          JSONObject json = (JSONObject) parser.parse(value);
          for (Object key : json.keySet()) {
            Object val = json.get(key);
            if ("link".equals(key)) {
              linkPut.addColumn(columnFamily, Bytes.toBytes(key.toString()),
                Bytes.toBytes(val.toString()));
            } else {
              infoPut.addColumn(columnFamily, Bytes.toBytes(key.toString()),
                Bytes.toBytes(val.toString()));
            }
          }
        }
        infoTable.mutate(infoPut); // co ParseJsonMulti-3-TwoTbls Save parsed values into two separate tables.
        linkTable.mutate(linkPut);
        context.getCounter(Counters.VALID).increment(1);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Error: " + e.getMessage() + ", Row: " +
          Bytes.toStringBinary(row.get()) + ", JSON: " + value);
        context.getCounter(Counters.ERROR).increment(1);
      }
    }
    // ^^ ParseJsonMulti
    /*
       {
         "updated": "Mon, 14 Sep 2009 17:09:02 +0000",
         "links": [{
           "href": "http://www.webdesigndev.com/",
           "type": "text/html",
           "rel": "alternate"
         }],
         "title": "Web Design Tutorials | Creating a Website | Learn Adobe
             Flash, Photoshop and Dreamweaver",
         "author": "outernationalist",
         "comments": "http://delicious.com/url/e104984ea5f37cf8ae70451a619c9ac0",
         "guidislink": false,
         "title_detail": {
           "base": "http://feeds.delicious.com/v2/rss/recent?min=1&count=100",
           "type": "text/plain",
           "language": null,
           "value": "Web Design Tutorials | Creating a Website | Learn Adobe
               Flash, Photoshop and Dreamweaver"
         },
         "link": "http://www.webdesigndev.com/",
         "source": {},
         "wfw_commentrss": "http://feeds.delicious.com/v2/rss/url/
             e104984ea5f37cf8ae70451a619c9ac0",
         "id": "http://delicious.com/url/
             e104984ea5f37cf8ae70451a619c9ac0#outernationalist"
       }
    */
    // vv ParseJsonMulti
  }

  // ^^ ParseJsonMulti
  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the parameters fails.
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    Option o = new Option("i", "input", true,
      "table to read from (must exist)");
    o.setArgName("input-table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("c", "column", true,
      "column to read data from (must exist)");
    o.setArgName("family:qualifier");
    options.addOption(o);
    o = new Option("o", "infotbl", true,
      "info table to write to (must exist)");
    o.setArgName("info-table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("l", "linktbl", true,
      "link table to write to (must exist)");
    o.setArgName("link-table-name");
    o.setRequired(true);
    options.addOption(o);
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }
    if (cmd.hasOption("d")) {
      Logger log = Logger.getLogger("mapreduce");
      log.setLevel(Level.DEBUG);
      System.out.println("DEBUG ON");
    }
    return cmd;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  // vv ParseJsonMulti
  public static void main(String[] args) throws Exception {
    /*...*/
    // ^^ ParseJsonMulti
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();
    CommandLine cmd = parseArgs(otherArgs);
    // check debug flag and other options
    if (cmd.hasOption("d")) conf.set("conf.debug", "true");
    // get details
    String input = cmd.getOptionValue("i");
    String column = cmd.getOptionValue("c");
    // vv ParseJsonMulti
    conf.set("conf.infotable", cmd.getOptionValue("o")); // co ParseJsonMulti-4-Conf Store table names in configuration for later use in the mapper.
    conf.set("conf.linktable", cmd.getOptionValue("l"));
    /*...*/
    // ^^ ParseJsonMulti
    Scan scan = new Scan();
    if (column != null) {
      byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
      if (colkey.length > 1) {
        scan.addColumn(colkey[0], colkey[1]);
        conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
        conf.set("conf.columnqualifier", Bytes.toStringBinary(colkey[1]));
      } else {
        scan.addFamily(colkey[0]);
        conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
      }
    }
    // vv ParseJsonMulti
    Job job = Job.getInstance(conf, "Parse data in " + input +
      ", into two tables");
    job.setJarByClass(ParseJsonMulti.class);
    TableMapReduceUtil.initTableMapperJob(input, scan, ParseMapper.class,
      ImmutableBytesWritable.class, Put.class, job);
    job.setOutputFormatClass(NullOutputFormat.class); // co ParseJsonMulti-5-Null Set the output format to be ignored by the framework.
    job.setNumReduceTasks(0);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  // ^^ ParseJsonMulti
}
