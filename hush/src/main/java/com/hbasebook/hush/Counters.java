package com.hbasebook.hush;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.model.ColumnQualifier;
import com.hbasebook.hush.model.Counter;
import com.hbasebook.hush.model.ShortUrl;
import com.hbasebook.hush.model.ShortUrlStatistics;
import com.hbasebook.hush.model.StatisticsCategory;
import com.hbasebook.hush.model.TimeFrame;
import com.hbasebook.hush.servlet.RequestInfo;
import com.hbasebook.hush.table.ShortUrlTable;
import com.maxmind.geoip.Country;

public class Counters {
  private final Log LOG = LogFactory.getLog(Counters.class);
  private final ResourceManager rm;

  Counters(ResourceManager rm) throws IOException {
    this.rm = rm;
  }

  /**
   * Increments the usage statistics of a shortened URL.
   *
   * @param shortId The shortId to increment.
   * @param info The request information, may be <code>null</code>.
   * @throws IOException When updating the counter fails.
   */
  public void incrementUsage(String shortId, RequestInfo info)
    throws IOException {
    incrementUsage(shortId, info, 1L, new Date());
  }

  /**
   * Increments the usage statistics of a shortened URL.
   *
   * @param shortId The shortId to increment.
   * @param info The request information, may be <code>null</code>.
   * @param incrBy The increment value.
   * @throws IOException When updating the counter fails.
   */
  public void incrementUsage(String shortId, RequestInfo info, long incrBy)
  throws IOException {
    incrementUsage(shortId, info, incrBy, new Date());
  }

  /**
   * Increments the usage statistics of a shortened URL.
   *
   * @param shortId The shortId to increment.
   * @param info The request information, may be <code>null</code>.
   * @param incrBy The increment value.
   * @param date  The date to use for the increment.
   * @throws IOException When updating the counter fails.
   */
  public void incrementUsage(String shortId, RequestInfo info, long incrBy,
      Date date) throws IOException {
    Country country = null;
    if (info != null) {
      country = rm.getCountry(info.get(RequestInfo.InfoName.RemoteAddr));
    }
    // increment user statistics
    Table table = rm.getTable(ShortUrlTable.NAME);
    byte[] rowKey = Bytes.toBytes(shortId);
    Increment increment = new Increment(rowKey);
    increment.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.CLICKS,
      incrBy);
    addIncrement(increment, StatisticsCategory.CLICK, date, null, incrBy);
    if (country != null) {
      addIncrement(increment, StatisticsCategory.COUNTRY, date,
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
  private void addIncrement(Increment increment, StatisticsCategory category,
    Date date, String extra, long incrBy) {
    byte[] qualifier = getQualifier(ColumnQualifier.DAY, category, date, extra);
    increment.addColumn(ShortUrlTable.DAILY_FAMILY, qualifier, incrBy);
    qualifier = getQualifier(ColumnQualifier.WEEK, category, date, extra);
    increment.addColumn(ShortUrlTable.WEEKLY_FAMILY, qualifier, incrBy);
    qualifier = getQualifier(ColumnQualifier.MONTH, category, date, extra);
    increment.addColumn(ShortUrlTable.MONTHLY_FAMILY, qualifier, incrBy);
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
    StatisticsCategory category, Date date, String extra) {
    byte[] result = qualifier.getColumnName(date, category);
    if (extra != null) {
      result = Bytes.add(result, ResourceManager.ZERO, Bytes.toBytes(extra));
    }
    return result;
  }

  public List<ShortUrlStatistics> getUserShortUrlStatistics(String username)
    throws IOException {
    List<ShortUrlStatistics> stats = new ArrayList<ShortUrlStatistics>();
    for (ShortUrl surl : rm.getUrlManager().getShortUrlsByUser(username)) {
      ShortUrlStatistics stat = getDailyStatistics(surl, 30, 110.0);
      if (stat != null) stats.add(stat);
    }
    return stats;
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
    ResourceManager rm = ResourceManager.getInstance();
    Table table = rm.getTable(ShortUrlTable.NAME);

    // get short Id usage data
    byte[] rowKey = Bytes.toBytes(shortUrl.getId());
    Get get = new Get(rowKey);
    get.addFamily(ShortUrlTable.DAILY_FAMILY);
    Result userShortUrlResult = table.get(get);
    if (userShortUrlResult.isEmpty()) {
      return null;
    }

    // create container to hold the computed values
    NavigableSet<Counter<Date, Double>> clicks =
      new TreeSet<Counter<Date, Double>>();
    Map<String, Counter<String, Long>> clicksByCountry =
      new TreeMap<String, Counter<String, Long>>();
    double maxValue = 0L;
    // iterate over usage data, sort descending (newest to oldest)
    Map<byte[], byte[]> familyMap =
      userShortUrlResult.getFamilyMap(ShortUrlTable.DAILY_FAMILY)
        .descendingMap();
    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
      // stop if we have enough values
      if (maxValues > 0 && clicks.size() >= maxValues) {
        break;
      }
      // parse the qualifier back into its details
      String[] kp = Bytes.toString(entry.getKey()).split("\u0000");
      StatisticsCategory category =
        StatisticsCategory.forCode(kp[1].charAt(0));
      double clickCount = Bytes.toLong(entry.getValue());
      switch (category) {
        case CLICK:
          maxValue = Math.max(maxValue, clickCount);
          try {
            clicks.add(
              new Counter<Date, Double>(ColumnQualifier.DAY.parseDate(kp[0]),
                new Double(clickCount), Counter.Sort.KeyDesc));
          } catch (ParseException e) {
            throw new IOException(e);
          }
          break;
        case COUNTRY:
          Counter<String, Long> countryCount = clicksByCountry.get(kp[2]);
          if (countryCount == null) {
            countryCount =
              new Counter<String, Long>(kp[2], Math.round(clickCount),
                Counter.Sort.ValueDesc);
          } else {
            countryCount.setValue(new Long(
              Math.round(clickCount) + countryCount.getValue().longValue()));
          }
          clicksByCountry.put(kp[2], countryCount);
      }
    }
    // optionally normalize the data
    if (normalize > 0) {
      normalizeData(clicks, normalize, maxValue);
    }

    rm.putTable(table);

    ShortUrlStatistics statistics = new ShortUrlStatistics(shortUrl,
      TimeFrame.DAY);
    statistics.addCounters("clicks", clicks);
    statistics.addCounters("clicksbycountry",
      new TreeSet<Counter<String, Long>>(clicksByCountry.values()));
    return statistics;
  }

  /**
   * Normalizes the given values, based on a normalization factor and the
   * maximum value seen.
   *
   * @param data The data to normalize.
   * @param normalize The factor to normalize to.
   * @param maxValue The maximum value in the data.
   */
  private void normalizeData(Set<Counter<Date, Double>> data, double normalize,
    double maxValue) {
    for (Counter<Date, Double> counter : data) {
      counter.setValue(new Double(counter.getValue().doubleValue() *
        normalize / maxValue));
    }
  }
}
