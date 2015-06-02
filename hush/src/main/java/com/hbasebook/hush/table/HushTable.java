package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class HushTable {
  public static final TableName NAME = TableName.valueOf("hush", "hush");
  public static final byte[] COUNTERS_FAMILY = Bytes.toBytes("cnt");
  public static final byte[] SHORT_ID = Bytes.toBytes("sid");
  public static final byte[] ANONYMOUS_USER_ID = Bytes.toBytes("auid");
  public static final byte[] GLOBAL_ROW_KEY = Bytes.toBytes("global");
}
