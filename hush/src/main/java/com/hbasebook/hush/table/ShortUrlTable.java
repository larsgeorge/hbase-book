package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class ShortUrlTable {
  public static final TableName NAME = TableName.valueOf("hush", "surl");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  public static final byte[] DAILY_FAMILY = Bytes.toBytes("std");
  public static final byte[] WEEKLY_FAMILY = Bytes.toBytes("stw");
  public static final byte[] MONTHLY_FAMILY = Bytes.toBytes("stm");
  public static final byte[] URL = Bytes.toBytes("url");
  public static final byte[] SHORT_DOMAIN = Bytes.toBytes("sdom");
  public static final byte[] REF_SHORT_ID = Bytes.toBytes("ref");
  public static final byte[] USER_ID = Bytes.toBytes("uid");
  public static final byte[] CLICKS = Bytes.toBytes("clk");
}
