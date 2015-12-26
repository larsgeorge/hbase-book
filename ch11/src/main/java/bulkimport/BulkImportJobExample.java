package bulkimport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class BulkImportJobExample {

  private static Log LOG = LogFactory.getLog(BulkImportJobExample.class);

  final static String SKIP_LINES_CONF_KEY = "skip.bad.lines";

  /**
   * Verbose input sampler. Allows to get some insight into the sampling process.
   *
   * @param <K> The key type.
   * @param <V> The value type.
   */
  public static class VerboseInputSampler<K, V> extends InputSampler<K, V> {
    private static Log LOG = LogFactory.getLog(VerboseInputSampler.class);

    public VerboseInputSampler(Configuration conf) {
      super(conf);
    }

    /**
     * Fixed a potential overlap of generated regions / splits for a dataset with lots of identical keys. For instance,
     * let your samples be: {1,1,1 ,1,3,3, 3,5,6} and your number of partitions be 3. Original implementation will get you
     * following splits, 1-1, 3-3, 3-6, notice the overlap between 2nd and 3rd partition.
     *
     * @param job
     * @param sampler
     * @param <K>
     * @param <V>
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public static <K, V> void writePartitionFile(Job job, InputSampler.Sampler<K, V> sampler)
      throws IOException, ClassNotFoundException, InterruptedException {
      LinkedList<K> splits = new LinkedList<K>();
      Configuration conf = job.getConfiguration();
      final InputFormat inf =
        ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
      int numPartitions = job.getNumReduceTasks();
      K[] samples = (K[])sampler.getSample(inf, job);
      LOG.info("Using " + samples.length + " samples");
      RawComparator<K> comparator = (RawComparator<K>) job.getGroupingComparator();
      Arrays.sort(samples, comparator);
      Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
      FileSystem fs = dst.getFileSystem(conf);
      if (fs.exists(dst)) fs.delete(dst, false);
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
      NullWritable nullValue = NullWritable.get();
      float stepSize = samples.length / (float) numPartitions;

      K lastKey = null;
      K currentKey = null;
      int lastKeyIndex = -1;
      for (int i = 1; i < numPartitions; ++i) {
        int currentKeyOffset = Math.round(stepSize * i);
        if (lastKeyIndex > currentKeyOffset) {
          long keyOffset = lastKeyIndex - currentKeyOffset;
          float errorRate = keyOffset / samples.length;
          LOG.warn(
            String.format("Partitions overlap. Consider using a different Sampler " +
              "and/or increase the number of samples and/or use more splits to take samples from. " +
              "Next sample would have been %s key overlaps by a distance of %d (factor %f) ", samples[currentKeyOffset], keyOffset, errorRate));
          currentKeyOffset = lastKeyIndex + 1;
        }
        currentKey = samples[currentKeyOffset];

        while (lastKey != null && comparator.compare(currentKey, lastKey) == 0) {
          currentKeyOffset++;
          if (currentKeyOffset >= samples.length) {
            LOG.info("Last 10 elements:");

            for (int d = samples.length - 1; d > samples.length - 11; d--) {
              LOG.debug(samples[d]);
            }
            throw new IOException("Not enough samples, stopped at partition " + i);
          }
          currentKey = samples[currentKeyOffset];
        }

        writer.append(currentKey, nullValue);
        lastKey = currentKey;
        lastKeyIndex = currentKeyOffset;
        splits.add(currentKey);
      }
      writer.close();
      LOG.info("*********************************************  ");
      LOG.info(" START KEYs for new Regions:  ");
      for (K split : splits) {
        LOG.info("* " + split.toString());
      }

    }

    public static class VerboseRandomSampler<K, V> implements Sampler<K, V> {
      int numSamples;
      int maxSplitsSampled;
      double freq;

      public VerboseRandomSampler(double freq, int numSamples) {
        this.freq = freq;
        this.numSamples = numSamples;
      }

      public VerboseRandomSampler(double freq, int numSamples, int maxSplitsSampled) {
        this.freq = freq;
        this.numSamples = numSamples;
        this.maxSplitsSampled = maxSplitsSampled;
      }

      public Object[] getSample(InputFormat inf, Job job) throws IOException, InterruptedException {
        long counter = 0;
        List<InputSplit> splits = inf.getSplits(job);
        ArrayList<K> samples = new ArrayList<K>(numSamples);
        int splitsToSample = Math.min(maxSplitsSampled, splits.size());

        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        LOG.debug("Seed: " + seed);
        // shuffle splits
        for (int i = 0; i < splits.size(); ++i) {
          InputSplit tmp = splits.get(i);
          int j = r.nextInt(splits.size());
          splits.set(i, splits.get(j));
          splits.set(j, tmp);
        }

        LOG.info(String.format("tTaking %d samples with frequency: %f and maximum splits: %d", numSamples, freq, maxSplitsSampled));

        // our target rate is in terms of the maximum number of sample splits,
        // but we accept the possibility of sampling additional splits to hit
        // the target sample keyset
        for (int i = 0; i < splitsToSample || (i < splits.size() && samples.size() < numSamples); ++i) {
          TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
          RecordReader<K, V> reader = inf.createRecordReader(splits.get(i), samplingContext);
          reader.initialize(splits.get(i), samplingContext);
          while (reader.nextKeyValue()) {
            if (r.nextDouble() <= freq) {
              if (samples.size() < numSamples) {
                if (counter % 1000 == 0)
                  LOG.info(String.format("Fill: Collected %d samples from %d splits", counter, i));
                counter++;
                samples.add(ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentKey(), null));
              } else {
                // When exceeding the maximum number of samples, replace a
                // random element with this one, then adjust the frequency
                // to reflect the possibility of existing elements being
                // pushed out
                int ind = r.nextInt(numSamples);
                if (ind != numSamples) {
                  samples.set(ind, ReflectionUtils.copy(job.getConfiguration(),
                    reader.getCurrentKey(), null));
                  if (counter % 1000 == 0)
                    LOG.info(String.format("Replace Random: Collected %d samples from %d splits", counter, i));
                  counter++;
                }
                freq *= (numSamples - 1) / (double) numSamples;
              }
            }
          }
          reader.close();
        }
        return (K[]) samples.toArray();
      }
    }
  }

  /**
   * Wrap a LineRecordReader to parse JSON data, line by line.
   */
  static class DeliciousRecordReader extends RecordReader<ImmutableBytesWritable, Put> {
    private LineRecordReader lineRecordReader = null;
    private JSONParser parser;
    private ImmutableBytesWritable currentKey = null;
    private Put currentValue = null;
    private boolean skipBadLines = true;
    private int badLineCount = 0;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      lineRecordReader = new LineRecordReader();
      lineRecordReader.initialize(inputSplit, taskAttemptContext);
      currentKey = new ImmutableBytesWritable();
      parser = new JSONParser();
      skipBadLines = taskAttemptContext.getConfiguration().getBoolean(
        SKIP_LINES_CONF_KEY, true);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean next = lineRecordReader.nextKeyValue();
      if (next) {
        String line = lineRecordReader.getCurrentValue().toString();
        try {
          JSONObject json = (JSONObject) parser.parse(line);
          String author = (String) json.get("author");
          String link = (String) json.get("link");

        } catch (ParseException e) {
          if (skipBadLines) {
            System.err.println("Bad line at offset: " +
              lineRecordReader.getCurrentKey().get() +
              ":\n" + e.getMessage());
            badLineCount++;
          } else {
            throw new IOException(e);
          }
        }
      }
      return next;
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Put getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
      lineRecordReader.close();
      if (badLineCount > 0) {
        System.err.println("Number of bad lines encountered: " + badLineCount);
      }
    }
  }

  /**
   * Dedicated input format to parse Delicious RSS feed data. Can be used
   * for the actual job, but also for the input sampler.
   */
  static class DeliciousInputFormat
    extends FileInputFormat<ImmutableBytesWritable, Put> {

    @Override
    public RecordReader<ImmutableBytesWritable, Put> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
      return new DeliciousRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
      return super.isSplitable(context, file);
    }
  }

  static class BulkImportMapper
    extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private JSONParser parser;
    private boolean skipBadLines;
    private Counter badLineCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      parser = new JSONParser();
      skipBadLines = context.getConfiguration().getBoolean(
        SKIP_LINES_CONF_KEY, true);
      badLineCount = context.getCounter("BulkImportJobExample", "Bad Lines");
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      try {
        Object o = parser.parse(line);

      } catch (ParseException e) {
        if (skipBadLines) {
          System.err.println("Bad line at offset: " + offset.get() +
            ":\n" + e.getMessage());
          badLineCount.increment(1);
          return;
        } else {
          throw new IOException(e);
        }
      }
    }
  }

  /*
      // "segs":{"cm.default":[["cm.drudge","0"]]}
      JSONObject networks = ((JSONObject) json).getJSONObject(OUT_SEGS);
      if (networks != null) {
        for (Iterator iter = networks.keys(); iter.hasNext(); ) {
          String network = (String) iter.next();
          JSONArray segs = (JSONArray) networks.get(network);
          if (segs != null) {
            for (int sv = 0; sv < segs.length(); sv++) {
              JSONArray seg = (JSONArray) segs.get(sv);
              rowKey.set(key);
              String line = key + "," + network + "," + seg.get(0) + "," + seg.get(1);
              if (LOG.isDebugEnabled()) LOG.debug("writeSegments: line -> " + line);
              rowValue.set(line);
              if (!dryrun) mos.write(OUT_SEGS, textFormat ? NullWritable.get() : rowKey, rowValue);
              context.getCounter(Counters.SEGMENTS).increment(1);
            }
          }
        }
      }
    }
{
    "updated": "Tue, 08 Sep 2009 23:28:55 +0000",
    "links": [
        {
            "href": "http://www.theatermania.com/broadway/",
            "type": "text/html",
            "rel": "alternate"
        }
    ],
    "title": "TheaterMania",
    "author": "mcasas1",
    "comments": "http://delicious.com/url/b5b3cbf9a9176fe43c27d7b4af94a422",
    "guidislink": false,
    "title_detail": {
        "base": "http://feeds.delicious.com/v2/rss/recent?min=1&count=100",
        "type": "text/plain",
        "language": null,
        "value": "TheaterMania"
    },
    "link": "http://www.theatermania.com/broadway/",
    "source": {

    },
    "wfw_commentrss": "http://feeds.delicious.com/v2/rss/url/b5b3cbf9a9176fe43c27d7b4af94a422",
    "id": "http://delicious.com/url/b5b3cbf9a9176fe43c27d7b4af94a422#mcasas1",
    "tags": [
        {
            "term": "NYC",
            "scheme": "http://delicious.com/mcasas1/",
            "label": null
        }
    ]
}
   */

  public static Job createSubmittableJob(Configuration conf, String[] args)
    throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    Path inputDir = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    boolean createPartitionFile = Boolean.parseBoolean(args[2]);

    Job job = Job.getInstance(conf,
      "Import delicious RSS feed into Hush tables.");
    job.setJarByClass(BulkImportJobExample.class);

    job.setInputFormatClass(TextInputFormat.class);
    // conf.setLong("hbase.hregion.max.filesize", 64 * 1024);
    FileInputFormat.setInputPaths(job, inputDir);

    job.setMapperClass(BulkImportMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    job.setPartitionerClass(TotalOrderPartitioner.class);

    job.setReducerClass(PutSortReducer.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    job.setOutputFormatClass(HFileOutputFormat.class);
    HFileOutputFormat.setOutputPath(job, outputDir);

    HFileOutputFormat.setCompressOutput(job, true);
    HFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    job.getConfiguration().set("hfile.compression", "gz");

    //job.getConfiguration().setFloat("mapred.job.shuffle.input.buffer.percent", 0.5f);
    //job.setNumReduceTasks(30);

    Path partitionsPath = new Path(job.getWorkingDirectory(),
      "partitions_" + System.currentTimeMillis());
    TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionsPath);

    if (createPartitionFile) {
      VerboseInputSampler.Sampler<KeyValue, ImmutableBytesWritable> sampler =
        new VerboseInputSampler.VerboseRandomSampler<KeyValue, ImmutableBytesWritable>(0.05, 1000000, 30);       // use 0.1 for real sampling

      LOG.info("Sampling key space");
      VerboseInputSampler.writePartitionFile(job, sampler);
      LOG.info("Samping done");
    }

    URI cacheUri = new URI(partitionsPath.toString() + "#" +
      TotalOrderPartitioner.DEFAULT_PATH);
    DistributedCache.addCacheFile(cacheUri, job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());

    return job;
  }

  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }

    System.err.println("Usage: ");
    System.err.println("foo.sh <input> <output> [flag]");
    System.err.println("  input: hdfs input directory");
    System.err.println("  output: hdfs output directory");
    System.err.println("  flag: true - create partitions file, false - do nothing.");

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    if (args.length < 3) {
      usage("Wrong number of arguments: " + args.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, args);
    if (job != null) System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
