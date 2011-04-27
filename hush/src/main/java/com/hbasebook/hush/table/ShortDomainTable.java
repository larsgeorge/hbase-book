package com.hbasebook.hush.table;

import org.apache.hadoop.hbase.util.Bytes;

public class ShortDomainTable {
  public static final byte[] NAME = Bytes.toBytes("sdom");
  public static final byte[] DOMAINS_FAMILY = Bytes.toBytes("domains");
}
