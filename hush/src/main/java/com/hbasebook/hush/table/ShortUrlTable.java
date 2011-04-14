package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.util.Bytes;

public class ShortUrlTable {
  public static final byte[] NAME = Bytes.toBytes("surl");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  public static final byte[] URL = Bytes.toBytes("url");
  public static final byte[] USER_ID = Bytes.toBytes("uid");

}
