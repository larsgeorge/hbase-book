package mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

// cc ImportJsonFromFile Example job that reads from a file and writes into a table.
// vv ImportJsonFromFile
public class ImportJsonFromFile {
  private static final Log LOG = LogFactory.getLog(ImportJsonFromFile.class);

  public static final String NAME = "ImportJsonFromFile";

  /**
   * Implements the <code>Mapper</code> that takes the lines from the input
   * and outputs <code>Put</code> instances.
   */
  static class ImportMapper
  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {

    private JSONParser parser = new JSONParser();

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

    /**
     * Maps the input.
     *
     * @param offset The current offset into the input file.
     * @param line The current line of the file.
     * @param context The task context.
     * @throws java.io.IOException When mapping the input fails.
     */
    @Override
    public void map(LongWritable offset, Text line, Context context)
    throws IOException {
      try {
        JSONObject json = (JSONObject) parser.parse(line.toString());
        String link = (String) json.get("link");
        byte[] md5Url = DigestUtils.md5(link);
        Put put = new Put(md5Url);
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("link"),
          Bytes.toBytes(link));
        context.write(new ImmutableBytesWritable(md5Url), put);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the
   *   parameters fails.
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    // create options
    Options options = new Options();
    Option o = new Option("t", "table", true,
      "table to import into (must exist)");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("i", "input", true,
      "the directory in DFS to read files from");
    o.setRequired(true);
    options.addOption(o);
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    // check if we are missing parameters
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    // check debug flag first
    if (cmd.hasOption("d")) {
      Logger log = Logger.getLogger("mapreduce");
      log.setLevel(Level.DEBUG);
    }
    return cmd;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    CommandLine cmd = parseArgs(otherArgs);
    // check debug flag and other options
    if (cmd.hasOption("d")) conf.set("conf.debug", "true");
    // get details
    String table = cmd.getOptionValue("t");
    String input = cmd.getOptionValue("i");
    // create job and set classes etc.
    Job job = Job.getInstance(conf, "Import from file " + input +
      " into table " + table);
    job.setJarByClass(ImportJsonFromFile.class);
    job.setMapperClass(ImportMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(input));
    // run the job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ ImportJsonFromFile
