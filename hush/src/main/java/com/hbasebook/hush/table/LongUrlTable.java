package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.util.Bytes;

public class LongUrlTable {
  public static final byte[] NAME = Bytes.toBytes("url");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  public static final byte[] URL = Bytes.toBytes("url");
  public static final byte[] SHORT_ID = Bytes.toBytes("sid");
  public static final byte[] USER_ID = Bytes.toBytes("uid");
}
