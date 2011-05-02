package com.hbasebook.hush;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.hbasebook.hush.servlet.RequestInfo;
import com.hbasebook.hush.table.*;
import com.maxmind.geoip.Country;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Counters {
  private final Log LOG = LogFactory.getLog(Counters.class);

  private static final byte[] ZERO = new byte[] {0};
  private static final byte[] DEFAULT_USER = Bytes.toBytes("@@@DEF");

  /**
   * All possible statistics saved in columns in the table.
   */
  public enum Category {
    Click("cl"),
    Country("co");

    private final String postfix;
    Category(String postfix) {
      this.postfix = postfix;
    }

    public String getPostfix() {
      return postfix;
    }

    @Override
    public String toString() {
      return postfix;
    }
  }

  /**
   * Time frame for statistics.
   */
  public enum TimeFrame {
    Day, Week, Month
  }

  /**
   * The column qualifiers for the statistics table.
   */
  public enum ColumnQualifier {
    Day("yyyyMMdd", TimeFrame.Day),
    Week("yyyyww", TimeFrame.Week),
    Month("yyyyMM", TimeFrame.Month);

    private final SimpleDateFormat formatter;
    private final TimeFrame timeFrame;

    ColumnQualifier(String format, TimeFrame timeFrame) {
      this.formatter = new SimpleDateFormat(format);
      this.timeFrame = timeFrame;
    }

    public byte[] getColumnName(Date date, Category type) {
      return Bytes.add(Bytes.toBytes(formatter.format(date)), ZERO,
        Bytes.toBytes(type.getPostfix()));
    }

    public TimeFrame getTimeFrame() {
      return timeFrame;
    }

    public Date parseDate(String date) throws ParseException {
      return formatter.parse(date);
    }
  }

  /**
   * Helps sorting the dates with newest first.
   */
  private class ReverseDateComparator implements Comparator<Date> {
    @Override
    public int compare(Date date1, Date date2) {
      return date2.compareTo(date1);
    }
  }

  /**
   * Container class to hold the details for the presentation layer.
   */
  public class ShortUrlStatistics {
    private final String shortId;
    private final String url;
    private final NavigableMap<Date, Double> clicks;
    private final TimeFrame timeFrame;

    private ShortUrlStatistics(String shortId, String url,
      NavigableMap<Date, Double> clicks, TimeFrame timeFrame) {
      this.shortId = shortId;
      this.url = url;
      this.clicks = clicks;
      this.timeFrame = timeFrame;
    }

    public String getShortId() {
      return shortId;
    }

    public String getUrl() {
      return url;
    }

    public NavigableMap<Date, Double> getClicks() {
      return clicks;
    }

    public TimeFrame getTimeFrame() {
      return timeFrame;
    }
  }

  /**
   * The digits used to BASE encode the short Ids.
   */
  private static final String baseDigits =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  /**
   * Encodes a number in BASE N.
   *
   * @param number  The number to encode.
   * @param base  The base to use for the encoding.
   * @param reverse  Flag to indicate if the result should be reversed.
   * @return The encoded - and optionally reversed - encoded string.
   */
  public static String longToString(long number, int base,
    boolean reverse) {
    String result = number == 0 ? "0" : "";
    while (number != 0) {
      int mod = (int) number % base;
      if (reverse) {
        result += baseDigits.charAt(mod);
      } else {
        result = baseDigits.charAt(mod) + result;
      }
      number = number / base;
    }
    return result;
  }

  /**
   * Decodes the given BASE N encoded value.
   *
   * @param number  The encoded value to decode.
   * @param base  The base to decode with.
   * @param reverse  Flag to indicate how the encoding was done.
   * @return The decoded number.
   */
  public static long parseLong(String number, int base,
    boolean reverse) {
    int length = number.length();
    int index = length;
    int result = 0;
    int multiplier = 1;
    while (index-- > 0) {
      int pos = reverse ? number.length() - (index + 1) : index;
      result += baseDigits.indexOf(number.charAt(pos)) * multiplier;
      multiplier = multiplier * base;
    }
    return result;
  }

  /**
   * Initialize the instance. This is done lazily as it requires global
   * resources that need to be setup first.
   *
   * @throws IOException When preparing the stored data fails.
   */
  public void init() throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Put put = new Put(HushTable.GLOBAL_ROW_KEY);
      byte[] value = Bytes.toBytes(parseLong("7330", 62, false));
      put.add(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, value);
      boolean hasPut = table.checkAndPut(HushTable.GLOBAL_ROW_KEY,
        HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, null, put);
      if (hasPut) {
        LOG.info("Short Id counter initialized.");
      }
    } catch (Exception e) {
      LOG.error("Unable to initialize Short Id.", e);
      throw new IOException(e);
    } finally {
      try {
        manager.putTable(table);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  /**
   * Convenience method to retrieve a new short Id. Each call increments the
   * counter by one.
   *
   * @return The newly created short Id.
   * @throws Exception When communicating with HBase fails.
   */
  public String getShortId() throws IOException {
    return getShortId(1L);
  }

  /**
   * Creates a new short Id.
   *
   * @param incrBy  The increment value.
   * @return The newly created short id, encoded as String.
   * @throws IOException When the counter fails to increment.
   */
  public String getShortId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID,
        incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(HushTable.COUNTERS_FAMILY,
        HushTable.SHORT_ID));
      return longToString(id, 62, true);
    } catch (Exception e) {
      LOG.error("Unable to a new short Id.", e);
      throw new IOException(e);
    } finally {
      try {
        manager.putTable(table);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  /**
   * Increments the usage statistics of a shortened URL.
   *
   *
   * @param shortUrl The short URL details.
   * @param info The request information, may be <code>null</code>.
   * @throws IOException When updating the counter fails.
   */
  public void incrementUsage(ShortUrl shortUrl, RequestInfo info)
    throws IOException {
    incrementUsage(shortUrl, info, 1L);
  }

  /**
   * Increments the usage statistics of a shortened URL.
   *
   * @param shortUrl The short URL details.
   * @param info The request information, may be <code>null</code>.
   * @param incrBy The increment value.
   * @throws IOException When updating the counter fails.
   */
  public void incrementUsage(ShortUrl shortUrl, RequestInfo info,
    long incrBy) throws IOException {
    Date date = new Date();
    ResourceManager manager = ResourceManager.getInstance();
    Country country = null;
    if (info != null) {
      country = manager.getCountry(info.get(RequestInfo.Name.RemoteAddr));
    }
    // increment user statistics
    HTable table = manager.getTable(UserShortUrlTable.NAME);
    byte[] rowKey = getRowKey(shortUrl);
    Increment increment = new Increment(rowKey);
    addIncrement(increment, Category.Click, date, null, incrBy);
    if (country != null) {
      addIncrement(increment, Category.Country, date, country.getCode(),
        incrBy);
    }
    table.increment(increment);
    manager.putTable(table);

    // increment URL statistics
    table = manager.getTable(LongUrlTable.NAME);
    rowKey = DigestUtils.md5(shortUrl.getLongUrl());
    increment = new Increment(rowKey);
    addIncrement(increment, Category.Click, date, null, incrBy);
    if (country != null) {
      addIncrement(increment, Category.Country, date, country.getCode(),
        incrBy);
    }
    table.increment(increment);
    manager.putTable(table);
  }

  /**
   * Adds all increments needed for a specific category, for all time ranges.
   *
   * @param increment  The increment instance to add to.
   * @param category  The category of the statistics.
   * @param date  The date component.
   * @param extra  An extra info element added to the counter (optionally).
   * @param incrBy  The increment value.
   */
  private void addIncrement(Increment increment, Category category, Date date,
    String extra, long incrBy) {
    byte[] qualifier = getQualifier(ColumnQualifier.Day, category, date, extra);
    increment.addColumn(UserShortUrlTable.DAILY_FAMILY, qualifier, incrBy);
    qualifier = getQualifier(ColumnQualifier.Week, category, date, extra);
    increment.addColumn(UserShortUrlTable.WEEKLY_FAMILY, qualifier, incrBy);
    qualifier = getQualifier(ColumnQualifier.Month, category, date, extra);
    increment.addColumn(UserShortUrlTable.MONTHLY_FAMILY, qualifier, incrBy);
  }

  /**
   * Creates the qualifier needed for the statistics columns.
   *
   * @param qualifier  The qualifier kind.
   * @param category  The statistics category.
   * @param date  The date for the statistics.
   * @param extra  The optional extra category element.
   * @return The qualifier.
   */
  private byte[] getQualifier(ColumnQualifier qualifier, Category category,
    Date date, String extra) {
    byte[] result = qualifier.getColumnName(date, category);
    if (extra != null) {
      result = Bytes.add(result, ZERO, Bytes.toBytes(extra));
    }
    return result;
  }

  /**
   * Helper to compute the row key for a shortened URL.
   *
   * @param shortUrl The current short URL details.
   * @return The row key in byte[] format.
   */
  public static byte[] getRowKey(ShortUrl shortUrl) {
    return Bytes.add(
      shortUrl.getUser() != null ? Bytes.toBytes(shortUrl.getUser()) :
        DEFAULT_USER, ZERO, Bytes.toBytes(shortUrl.getId()));
  }

  /**
   * Returns daily statistics for the given shortened URL.
   *
   * @param shortUrl The shortened URL.
   * @return The statistics.
   * @throws IOException When loading the statistics fails.
   */
  public ShortUrlStatistics getDailyStatistics(ShortUrl shortUrl)
    throws IOException {
    return getDailyStatistics(shortUrl, -1);
  }

  /**
   * Returns daily statistics for the given shortened URL.
   *
   * @param shortUrl The shortened URL.
   * @param maxValues The maximum number of values to return.
   * @return The statistics.
   * @throws IOException When loading the statistics fails.
   */
  public ShortUrlStatistics getDailyStatistics(ShortUrl shortUrl, int maxValues)
  throws IOException {
    return getDailyStatistics(shortUrl, maxValues, -1);
  }

  /**
   * Retrieves the daily clicks per short Id.
   *
   * @param shortUrl The shortened URL with details.
   * @param maxValues The maximum number of values to return, -1 means all.
   * @param normalize When > 0 then the data is normalized.
   * @return A container with the details.
   * @throws IOException When loading the data from HBase failed.
   */
  public ShortUrlStatistics getDailyStatistics(ShortUrl shortUrl, int maxValues,
    double normalize) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);
    HTable shortUrltable = manager.getTable(ShortUrlTable.NAME);

    // get short Id to URL mapping
    byte[] shortId = Bytes.toBytes(shortUrl.getId());
    Get get = new Get(shortId);
    Result shortUrlData = shortUrltable.get(get);
    String url = Bytes.toString(shortUrlData.getValue(ShortUrlTable.DATA_FAMILY,
      ShortUrlTable.URL));
    // get short Id usage data
    String user = shortUrl.getUser();
    if (user == null) {
      user = getUser(shortUrl.getId());
    }
    byte[] rowKey = Bytes.add(Bytes.toBytes(user), ZERO, shortId);
    get = new Get(rowKey);
    get.addFamily(UserShortUrlTable.DAILY_FAMILY);
    Result userShortUrlResult = userShortUrltable.get(get);
    if (userShortUrlResult.isEmpty()) {
      return null;
    }

    // TODO - Fix this once the reversed sorting is working
    NavigableMap<Date, Double> clicks = new TreeMap<Date, Double>(
      new ReverseDateComparator());
    double maxValue = 0L;
    Map<byte[], byte[]> familyMap = userShortUrlResult.getFamilyMap(
      UserShortUrlTable.DAILY_FAMILY).descendingMap();
    int count = 0;
    // iterate over usage data
    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
      if (maxValues > 0 && count++ >= maxValues) {
        break;
      }
      double clickCount = Bytes.toLong(entry.getValue());
      maxValue = Math.max(maxValue, clickCount);
      try {
        clicks.put(ColumnQualifier.Day.parseDate(Bytes.toString(
          entry.getKey())), new Double(clickCount));
      } catch (ParseException e) {
        throw new IOException(e);
      }
    }
    // optionally normalize the data
    if (normalize > 0) {
      clicks = normalizeData(clicks, normalize, maxValue);
    }

    manager.putTable(userShortUrltable);
    manager.putTable(shortUrltable);

    return new ShortUrlStatistics(shortUrl.getId(), url, clicks,
      TimeFrame.Day);
  }

  /**
   * Loads a user based on a short Id.
   *
   * @param shortId  The short Id to get the user for.
   * @return The user name or <code>null</code> if not found.
   * @throws IOException When loading the user fails.
   */
  private String getUser(String shortId) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    ShortUrl shortUrl = manager.getUrlManager().getShortUrl(shortId);
    return shortUrl != null ? shortUrl.getUser() : null;
  }

  /**
   * Normalizes the given values, based on a normalization factor and
   * the maximum value seen.
   *
   * @param data The data to normalize.
   * @param normalize The factor to normalize to.
   * @param maxValue The maximum value in the data.
   * @return A copy of the list with all values normalized.
   */
  private NavigableMap<Date, Double> normalizeData(Map<Date,
    Double> data, double normalize, double maxValue) {
    NavigableMap<Date, Double> norms = new TreeMap<Date, Double>(
      new ReverseDateComparator());
    for (Map.Entry<Date, Double> entry : data.entrySet()) {
      norms.put(entry.getKey(),
        new Double(entry.getValue() * normalize / maxValue));
    }
    return norms;
  }
}
