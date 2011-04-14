package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.util.Bytes;

public class UserShortUrlTable {
  public static final byte[] NAME = Bytes.toBytes("user-surl");
  public static final byte[] DAILY_FAMILY = Bytes.toBytes("std");
  public static final byte[] WEEKLY_FAMILY = Bytes.toBytes("stw");
  public static final byte[] MONTHLY_FAMILY = Bytes.toBytes("stm");
}
