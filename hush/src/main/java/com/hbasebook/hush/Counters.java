package com.hbasebook.hush;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.ShortUrlTable;
import com.hbasebook.hush.table.UserShortUrlTable;

public class Counters {
  private final Log LOG = LogFactory.getLog(Counters.class);

  private static final byte[] ZERO = new byte[] { 0 };
  private static final byte[] DEFAULT_USER = Bytes.toBytes("@@@DEF");

  public enum TimeFrame {
    Day, Week, Month
  }

  public enum ColumnQualifier {
    DAY("yyyyMMdd", TimeFrame.Day), WEEK("ww", TimeFrame.Week), MONTH("yyyyMM",
        TimeFrame.Month);

    private final SimpleDateFormat formatter;
    private final TimeFrame timeFrame;

    ColumnQualifier(String format, TimeFrame timeFrame) {
      this.formatter = new SimpleDateFormat(format);
      this.timeFrame = timeFrame;
    }

    public byte[] getColumnName(Date date) {
      return Bytes.toBytes(formatter.format(date));
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

  private static final String baseDigits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  public static String longToString(long number, int base, boolean reverse) {
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
   * Convenience method to retrieve a new short Id. The value is returned in the
   * proper format to be used as a row key in the HBase table. Each call
   * increments the counter by one.
   * 
   * @return The newly created short Id.
   * @throws Exception
   *           When communicating with HBase fails.
   */
  public byte[] getShortId() throws IOException {
    return getShortId(1L);
  }

  public byte[] getShortId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment
          .addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(HushTable.COUNTERS_FAMILY,
          HushTable.SHORT_ID));
      return Bytes.toBytes(longToString(id, 62, true));
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
   * Convenience method to pass in Strings instead of byte arrays.
   * 
   * @param user
   * @param shortId
   * @throws IOException
   */
  public void incrementUsage(String user, String shortId) throws IOException {
    incrementUsage(Bytes.toBytes(user), Bytes.toBytes(shortId));
  }

  public void incrementUsage(byte[] user, byte[] shortId) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(UserShortUrlTable.NAME);
    Date date = new Date();
    if (user == null) {
      user = DEFAULT_USER;
    }
    byte[] rowKey = getRowKey(user, shortId);
    Increment increment = new Increment(rowKey);
    increment.addColumn(UserShortUrlTable.DAILY_FAMILY, ColumnQualifier.DAY
        .getColumnName(date), 1L);
    increment.addColumn(UserShortUrlTable.WEEKLY_FAMILY, ColumnQualifier.WEEK
        .getColumnName(date), 1L);
    increment.addColumn(UserShortUrlTable.MONTHLY_FAMILY, ColumnQualifier.MONTH
        .getColumnName(date), 1L);
    table.increment(increment);
    manager.putTable(table);
  }

  private byte[] getRowKey(byte[] user, byte[] shortId) {
    return Bytes.add(user, ZERO, shortId);
  }

  public ShortUrlStatistics getDailyStatistics(byte[] user, byte[] shortId)
      throws IOException {
    return getDailyStatistics(user, shortId, -1);
  }

  public ShortUrlStatistics getDailyStatistics(byte[] user, byte[] shortId,
      int maxValues) throws IOException {
    return getDailyStatistics(user, shortId, maxValues, -1);
  }

  public ShortUrlStatistics getDailyStatistics(String user, String shortId,
      int maxValues, double normalize) throws IOException {
    return getDailyStatistics(Bytes.toBytes(user), Bytes.toBytes(shortId),
        maxValues, normalize);
  }

  /**
   * Retrieves the daily clicks per short Id.
   * 
   * @param user
   *          The user owning the short Id.
   * @param shortId
   *          The short Id.
   * @param maxValues
   *          The maximum number of values to return, -1 means all.
   * @param normalize
   *          When > 0 then the data is normalized.
   * @return A container with the details.
   * @throws IOException
   *           When loading the data from HBase failed.
   */
  public ShortUrlStatistics getDailyStatistics(byte[] user, byte[] shortId,
      int maxValues, double normalize) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);
    HTable shortUrltable = manager.getTable(ShortUrlTable.NAME);

    // get short Id to URL mapping
    Get get = new Get(shortId);
    Result shortUrlData = shortUrltable.get(get);
    String url = Bytes.toString(shortUrlData.getValue(
        ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL));
    // get short Id usage data
    byte[] rowKey = Bytes.add(user, ZERO, shortId);
    get = new Get(rowKey);
    get.addFamily(UserShortUrlTable.DAILY_FAMILY);
    Result userShortUrlResult = userShortUrltable.get(get);

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
        clicks.put(ColumnQualifier.DAY
            .parseDate(Bytes.toString(entry.getKey())), new Double(clickCount));
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

    return new ShortUrlStatistics(Bytes.toString(shortId), url, clicks,
        TimeFrame.Day);
  }

  /**
   * Normalizes the given values, based on a normalization factor and the
   * maximum value seen.
   * 
   * @param data
   *          The data to normalize.
   * @param normalize
   *          The factor to normalize to.
   * @param maxValue
   *          The maximum value in the data.
   * @return A copy of the list with all values normalized.
   */
  private NavigableMap<Date, Double> normalizeData(Map<Date, Double> data,
      double normalize, double maxValue) {
    NavigableMap<Date, Double> norms = new TreeMap<Date, Double>(
        new ReverseDateComparator());
    for (Map.Entry<Date, Double> entry : data.entrySet()) {
      norms.put(entry.getKey(), new Double(entry.getValue() * normalize
          / maxValue));
    }
    return norms;
  }
}
