package com.hbasebook.hush.table;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class UserTable {
  public static final TableName NAME = TableName.valueOf("hush", "user");
  public static final byte[] DATA_FAMILY = Bytes.toBytes("data");

  public static final byte[] CREDENTIALS = Bytes.toBytes("credentials");
  public static final byte[] ROLES = Bytes.toBytes("roles");

  public static final byte[] ADMIN_ROLE = Bytes.toBytes("admin");
  public static final byte[] USER_ROLE = Bytes.toBytes("user");
  public static final byte[] ADMIN_ROLES = Bytes.add(ADMIN_ROLE,
    Bytes.toBytes(","), USER_ROLE);

  public static final byte[] FIRSTNAME = Bytes.toBytes("firstname");
  public static final byte[] LASTNAME = Bytes.toBytes("lastname");
  public static final byte[] EMAIL = Bytes.toBytes("email");

  public void addUser(String username) throws IOException {
  }

}
