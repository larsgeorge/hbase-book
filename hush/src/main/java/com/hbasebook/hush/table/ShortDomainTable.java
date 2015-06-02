package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class ShortDomainTable {
  public static final TableName NAME = TableName.valueOf("hush", "sdom");
  public static final byte[] DOMAINS_FAMILY = Bytes.toBytes("domains");
}
