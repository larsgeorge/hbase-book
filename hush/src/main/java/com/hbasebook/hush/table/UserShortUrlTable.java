package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.util.Bytes;

public class UserShortUrlTable {
  public static final byte[] NAME = Bytes.toBytes("user-surl");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  public static final byte[] TIMESTAMP = Bytes.toBytes("ts");
}
