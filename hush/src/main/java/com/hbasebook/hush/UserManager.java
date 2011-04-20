package com.hbasebook.hush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.table.User;
import com.hbasebook.hush.table.UserTable;

public class UserManager {
  private final Log LOG = LogFactory.getLog(UserManager.class);
  private final ResourceManager rm;

  UserManager(ResourceManager rm) throws IOException {
    this.rm = rm;
  }

  public void createAdminUser() throws IOException {
    HTable table = rm.getTable(UserTable.NAME);
    try {
      byte[] ADMIN_LOGIN = Bytes.toBytes("admin");
      byte[] ADMIN_PASSWORD = ADMIN_LOGIN;

      Put put = new Put(ADMIN_LOGIN);
      put.add(UserTable.DATA_FAMILY, UserTable.CREDENTIALS, ADMIN_PASSWORD);
      put.add(UserTable.DATA_FAMILY, UserTable.ROLES, UserTable.ADMIN_ROLES);
      boolean hasPut = table.checkAndPut(ADMIN_LOGIN, UserTable.DATA_FAMILY,
          UserTable.ROLES, null, put);
      if (hasPut) {
        LOG.info("Admin user initialized.");
      }
    } catch (Exception e) {
      LOG.error("Unable to initialize admin user.", e);
      throw new IOException(e);
    } finally {
      rm.putTable(table);
    }
  }

  public User getUser(String username) {
    User user = null;
    HTable table = null;
    try {
      table = rm.getTable(UserTable.NAME);
      Get get = new Get(Bytes.toBytes(username));
      get.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS);
      get.addColumn(UserTable.DATA_FAMILY, UserTable.ROLES);

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }

      String firstName = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.FIRSTNAME));
      String lastName = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.LASTNAME));
      String email = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.EMAIL));
      String credentials = Bytes.toString(result.getValue(
          UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
      String roleString = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.ROLES));
      String[] roles = roleString == null ? null : roleString.split(",");
      user = new User(username, firstName, lastName, email, credentials, roles);
    } catch (Exception e) {
      LOG.error(String.format("Unable to get user '%s'", username), e);
    } finally {
      rm.putTable(table);
    }

    return user;
  }

  public List<User> getUsers() throws IOException {
    List<User> users = new ArrayList<User>();
    HTable table = rm.getTable(UserTable.NAME);

    Scan scan = new Scan();
    scan.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS);
    scan.addColumn(UserTable.DATA_FAMILY, UserTable.ROLES);
    ResultScanner scanner = table.getScanner(scan);

    Iterator<Result> results = scanner.iterator();
    int errors = 0;
    while (results.hasNext()) {
      Result result = results.next();
      if (!result.isEmpty()) {
        try {
          String username = Bytes.toString(result.getRow());
          String firstName = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.FIRSTNAME));
          String lastName = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.LASTNAME));
          String email = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
              UserTable.EMAIL));
          String credentials = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
          String roleString = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.ROLES));
          String[] roles = roleString == null ? null : roleString.split(",");
          User user = new User(username, firstName, lastName, email,
              credentials, roles);
          users.add(user);
        } catch (Exception e) {
          errors++;
        }
      }
    }
    if (errors > 0) {
      LOG.error(String.format("Encountered %d errors in getUsers", errors));
    }
    rm.putTable(table);
    return users;
  }
}
