package com.hbasebook.hush;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.model.Category;
import com.hbasebook.hush.model.ColumnQualifier;
import com.hbasebook.hush.model.ShortUrl;
import com.hbasebook.hush.model.ShortUrlStatistics;
import com.hbasebook.hush.model.TimeFrame;
import com.hbasebook.hush.servlet.RequestInfo;
import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.LongUrlTable;
import com.hbasebook.hush.table.UserShortUrlTable;
import com.maxmind.geoip.Country;

public class Counters {
  private final Log LOG = LogFactory.getLog(Counters.class);
  private final ResourceManager rm;

  // TODO: really never used anymore... yank it?
  private static final byte[] DEFAULT_USER = Bytes.toBytes("@@@DEF");

  Counters(ResourceManager rm) throws IOException {
    this.rm = rm;
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
   * The digits used to BASE encode the short Ids.
   */
  private static final String baseDigits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  /**
   * Encodes a number in BASE N.
   * 
   * @param number The number to encode.
   * @param base The base to use for the encoding.
   * @param reverse Flag to indicate if the result should be reversed.
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
   * @param number The encoded value to decode.
   * @param base The base to decode with.
   * @param reverse Flag to indicate how the encoding was done.
   * @return The decoded number.
   */
  public static long parseLong(String number, int base, boolean reverse) {
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

      put = new Put(HushTable.GLOBAL_ROW_KEY);
      put.add(HushTable.COUNTERS_FAMILY, HushTable.ANONYMOUS_USER_ID,
          Bytes.toBytes(parseLong("0", 62, false)));
      hasPut = table.checkAndPut(HushTable.GLOBAL_ROW_KEY,
          HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, null, put);
      if (hasPut) {
        LOG.info("Anonymous User Id counter initialized.");
      }
    } catch (Exception e) {
      LOG.error("Unable to initialize counters.", e);
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
   * @param incrBy The increment value.
   * @return The newly created short id, encoded as String.
   * @throws IOException When the counter fails to increment.
   */
  public String getShortId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY,
          HushTable.SHORT_ID, incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(HushTable.COUNTERS_FAMILY,
          HushTable.SHORT_ID));
      return longToString(id, 62, true);
    } catch (Exception e) {
      LOG.error("Unable to create a new short Id.", e);
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
   * Convenience method to retrieve a new anonymous User Id. Each call
   * increments the counter by one.
   * 
   * @return The newly created user Id.
   * @throws Exception When communicating with HBase fails.
   */
  public String getAnonymousUserId() throws IOException {
    return getAnonymousUserId(1L);
  }

  /**
   * Creates a new short Id.
   * 
   * @param incrBy The increment value.
   * @return The newly created short id, encoded as String.
   * @throws IOException When the counter fails to increment.
   */
  public String getAnonymousUserId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY,
          HushTable.ANONYMOUS_USER_ID, incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(HushTable.COUNTERS_FAMILY,
          HushTable.ANONYMOUS_USER_ID));
      return longToString(id, 62, true);
    } catch (Exception e) {
      LOG.error("Unable to create a new anonymous user Id.", e);
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
    Country country = null;
    if (info != null) {
      country = rm.getCountry(info.get(RequestInfo.Name.RemoteAddr));
    }
    // increment user statistics
    HTable table = rm.getTable(UserShortUrlTable.NAME);
    byte[] rowKey = getRowKey(shortUrl);
    Increment increment = new Increment(rowKey);
    addIncrement(increment, Category.Click, date, null, incrBy);
    if (country != null) {
      addIncrement(increment, Category.Country, date,
          country.getCode(), incrBy);
    }
    table.increment(increment);
    rm.putTable(table);

    // increment URL statistics
    table = rm.getTable(LongUrlTable.NAME);
    rowKey = DigestUtils.md5(shortUrl.getLongUrl());
    increment = new Increment(rowKey);
    addIncrement(increment, Category.Click, date, null, incrBy);
    if (country != null) {
      addIncrement(increment, Category.Country, date,
          country.getCode(), incrBy);
    }
    table.increment(increment);
    rm.putTable(table);
  }

  /**
   * Adds all increments needed for a specific category, for all time ranges.
   * 
   * @param increment The increment instance to add to.
   * @param category The category of the statistics.
   * @param date The date component.
   * @param extra An extra info element added to the counter (optionally).
   * @param incrBy The increment value.
   */
  private void addIncrement(Increment increment, Category category,
      Date date, String extra, long incrBy) {
    byte[] qualifier = getQualifier(ColumnQualifier.Day, category,
        date, extra);
    increment.addColumn(UserShortUrlTable.DAILY_FAMILY, qualifier,
        incrBy);
    qualifier = getQualifier(ColumnQualifier.Week, category, date,
        extra);
    increment.addColumn(UserShortUrlTable.WEEKLY_FAMILY, qualifier,
        incrBy);
    qualifier = getQualifier(ColumnQualifier.Month, category, date,
        extra);
    increment.addColumn(UserShortUrlTable.MONTHLY_FAMILY, qualifier,
        incrBy);
  }

  /**
   * Creates the qualifier needed for the statistics columns.
   * 
   * @param qualifier The qualifier kind.
   * @param category The statistics category.
   * @param date The date for the statistics.
   * @param extra The optional extra category element.
   * @return The qualifier.
   */
  private byte[] getQualifier(ColumnQualifier qualifier,
      Category category, Date date, String extra) {
    byte[] result = qualifier.getColumnName(date, category);
    if (extra != null) {
      result = Bytes.add(result, ResourceManager.ZERO,
          Bytes.toBytes(extra));
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
        shortUrl.getUser() != null ? Bytes.toBytes(shortUrl.getUser())
            : DEFAULT_USER, ResourceManager.ZERO,
        Bytes.toBytes(shortUrl.getId()));
  }

  public List<ShortUrlStatistics> getUserShortUrlStatistics(
      String username) throws IOException {
    HTable userShortUrlTable = rm.getTable(UserShortUrlTable.NAME);

    byte[] startRow = Bytes.toBytes(username);
    byte[] stopRow = Bytes.add(startRow, ResourceManager.ONE);

    Scan scan = new Scan(startRow, stopRow);
    scan.addFamily(UserShortUrlTable.DAILY_FAMILY);

    ResultScanner scanner = userShortUrlTable.getScanner(scan);
    List<ShortUrlStatistics> stats = new ArrayList<ShortUrlStatistics>();
    for (Result result : scanner) {
      String rowKey = Bytes.toString(result.getRow());
      String shortId = rowKey.substring(rowKey.indexOf(0) + 1);
      ShortUrl shortUrl = rm.getUrlManager().getShortUrl(shortId);
      ShortUrlStatistics stat = getDailyStatistics(shortUrl, 30, 110.0);
      stats.add(stat);
    }
    rm.putTable(userShortUrlTable);
    return stats;
  }

  /**
   * Returns daily statistics for the given shortened URL.
   * 
   * @param shortUrl The shortened URL.
   * @param maxValues The maximum number of values to return.
   * @return The statistics.
   * @throws IOException When loading the statistics fails.
   */
  public ShortUrlStatistics getDailyStatistics(ShortUrl shortUrl,
      int maxValues) throws IOException {
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
  public ShortUrlStatistics getDailyStatistics(ShortUrl shortUrl,
      int maxValues, double normalize) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);

    // get short Id usage data
    byte[] rowKey = Bytes.add(Bytes.toBytes(shortUrl.getUser()),
        ResourceManager.ZERO, Bytes.toBytes(shortUrl.getId()));
    Get get = new Get(rowKey);
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
        clicks.put(
            ColumnQualifier.Day.parseDate(Bytes.toString(entry.getKey())),
            new Double(clickCount));
      } catch (ParseException e) {
        throw new IOException(e);
      }
    }
    // optionally normalize the data
    if (normalize > 0) {
      clicks = normalizeData(clicks, normalize, maxValue);
    }

    manager.putTable(userShortUrltable);

    return new ShortUrlStatistics(shortUrl, clicks, TimeFrame.Day);
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
   * Normalizes the given values, based on a normalization factor and the
   * maximum value seen.
   * 
   * @param data The data to normalize.
   * @param normalize The factor to normalize to.
   * @param maxValue The maximum value in the data.
   * @return A copy of the list with all values normalized.
   */
  private NavigableMap<Date, Double> normalizeData(
      Map<Date, Double> data, double normalize, double maxValue) {
    NavigableMap<Date, Double> norms = new TreeMap<Date, Double>(
        new ReverseDateComparator());
    for (Map.Entry<Date, Double> entry : data.entrySet()) {
      norms.put(entry.getKey(), new Double(entry.getValue() * normalize
          / maxValue));
    }
    return norms;
  }
}
