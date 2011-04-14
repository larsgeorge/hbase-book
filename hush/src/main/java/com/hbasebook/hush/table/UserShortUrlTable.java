package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.util.Bytes;

public class UserShortUrlTable {
  public static final byte[] NAME = Bytes.toBytes("user");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  public static final byte[] CREDENTIALS = Bytes.toBytes("credentials");
  public static final byte[] ROLES = Bytes.toBytes("roles");

}
