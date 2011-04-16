package com.hbasebook.hush;

import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.UserShortUrlTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Counters {
  private final Log LOG = LogFactory.getLog(Counters.class);

  private static final byte[] ZERO = new byte[]{0};
  private static final byte[] DEFAULT_USER = Bytes.toBytes("@@@DEF");

  public enum ColumnQualifier {
    DAY("yyyyMMdd"),
    WEEK("ww"),
    MONTH("yyyyMM");

    private final SimpleDateFormat formatter;

    ColumnQualifier(String format) {
      this.formatter = new SimpleDateFormat(format);
    }

    public byte[] getColumnName(Date date) {
      return Bytes.toBytes(formatter.format(date));
    }
  }

  private static final String baseDigits =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

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
        HushTable.COUNTERS_FAMILY,
        HushTable.SHORT_ID, null, put);
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

  public byte[] getShortId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(
        HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID));
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

  public void incrementUsage(byte[] user, byte[] shortId) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(UserShortUrlTable.NAME);
    Date date = new Date();
    if (user == null) {
      user = DEFAULT_USER;
    }
    byte[] rowKey = getRowKey(user, shortId);
    Increment increment = new Increment(rowKey);
    increment.addColumn(UserShortUrlTable.DAILY_FAMILY,
      ColumnQualifier.DAY.getColumnName(date), 1L);
    increment.addColumn(UserShortUrlTable.WEEKLY_FAMILY,
      ColumnQualifier.WEEK.getColumnName(date), 1L);
    increment.addColumn(UserShortUrlTable.MONTHLY_FAMILY,
      ColumnQualifier.MONTH.getColumnName(date), 1L);
    table.increment(increment);
  }

  private byte[] getRowKey(byte[] user, byte[] shortId) {
    return Bytes.add(user, ZERO, shortId);
  }
}
