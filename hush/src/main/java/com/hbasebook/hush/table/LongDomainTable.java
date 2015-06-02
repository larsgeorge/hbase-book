package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class LongDomainTable {
  public static final TableName NAME = TableName.valueOf("hush", "ldom");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  public static final byte[] SHORT_DOMAIN = Bytes.toBytes("sdom");
}
