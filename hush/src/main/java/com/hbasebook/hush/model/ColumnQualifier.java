package com.hbasebook.hush.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.ResourceManager;


/**
 * The column qualifiers for the statistics table.
 */
public enum ColumnQualifier {
  Day("yyyyMMdd", TimeFrame.Day), Week("yyyyww", TimeFrame.Week), Month(
      "yyyyMM", TimeFrame.Month);

  private final SimpleDateFormat formatter;
  private final TimeFrame timeFrame;

  ColumnQualifier(String format, TimeFrame timeFrame) {
    this.formatter = new SimpleDateFormat(format);
    this.timeFrame = timeFrame;
  }

  public byte[] getColumnName(Date date, Category type) {
    return Bytes.add(Bytes.toBytes(formatter.format(date)), ResourceManager.ZERO,
        Bytes.toBytes(type.getPostfix()));
  }

  public TimeFrame getTimeFrame() {
    return timeFrame;
  }

  public Date parseDate(String date) throws ParseException {
    return formatter.parse(date);
  }
}