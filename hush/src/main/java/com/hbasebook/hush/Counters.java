package com.hbasebook.hush;

import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.UserShortUrlTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Counters {
  private final Log LOG = LogFactory.getLog(Counters.class);

  private static final byte[] ZERO = new byte[]{0};
  private static final String DEFAULT_USER = "@@@DEF";

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

  public byte[] getShortId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(
        HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID));
      return Bytes.toBytes(Long.toString(id, 36));
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

  public void incrementUsage(String user, byte[] shortId) throws IOException {
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

  private byte[] getRowKey(String user, byte[] shortId) {
    return Bytes.add(Bytes.toBytes(user), ZERO, shortId);
  }
}
