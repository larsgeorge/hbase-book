package com.hbasebook.hush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.model.User;
import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.UserTable;

public class UserManager {
  private final Log LOG = LogFactory.getLog(UserManager.class);
  private final ResourceManager rm;
  private static final String ANONYMOUS_SUFFIX = ":anon";

  UserManager(ResourceManager rm) throws IOException {
    this.rm = rm;
  }

  /**
   * Initialize the instance. This is done lazily as it requires global
   * resources that need to be setup first.
   * 
   * @throws IOException When preparing the stored data fails.
   */
  public void init() throws IOException {
    HTable table = rm.getTable(HushTable.NAME);
    try {
      Put put = new Put(HushTable.GLOBAL_ROW_KEY);
      put.add(HushTable.COUNTERS_FAMILY, HushTable.ANONYMOUS_USER_ID,
          Bytes.toBytes(HushUtil.hushDecode("0")));
      boolean hasPut = table.checkAndPut(HushTable.GLOBAL_ROW_KEY,
          HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, null, put);
      if (hasPut) {
        LOG.info("Anonymous User Id counter initialized.");
      }
      table.flushCommits();
    } catch (Exception e) {
      LOG.error("Unable to initialize counters.", e);
      throw new IOException(e);
    } finally {
      rm.putTable(table);
    }
  }

  public void createRootUser() throws IOException {
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

  // cc HushHTablePoolUsage Using the pool in application code
  // vv HushHTablePoolUsage
  public void createUser(String username, String firstName, String lastName,
      String email, String password, String roles) throws IOException {
    /* [ */HTable table = rm.getTable(UserTable.NAME);/* ] */
    Put put = new Put(Bytes.toBytes(username));
    put.add(UserTable.DATA_FAMILY, UserTable.FIRSTNAME,
        Bytes.toBytes(firstName));
    put.add(UserTable.DATA_FAMILY, UserTable.LASTNAME,
        Bytes.toBytes(lastName));
    put.add(UserTable.DATA_FAMILY, UserTable.EMAIL, Bytes.toBytes(email));
    put.add(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
        Bytes.toBytes(password));
    put.add(UserTable.DATA_FAMILY, UserTable.ROLES, Bytes.toBytes(roles));
    table.put(put);
    table.flushCommits();
    /* [ */rm.putTable(table);/* ] */
  }

  // ^^ HushHTablePoolUsage

  public void updateUser(String username, String firstName, String lastName,
      String email) throws IOException {
    HTable table = rm.getTable(UserTable.NAME);
    Put put = new Put(Bytes.toBytes(username));
    put.add(UserTable.DATA_FAMILY, UserTable.FIRSTNAME,
        Bytes.toBytes(firstName));
    put.add(UserTable.DATA_FAMILY, UserTable.LASTNAME,
        Bytes.toBytes(lastName));
    put.add(UserTable.DATA_FAMILY, UserTable.EMAIL, Bytes.toBytes(email));
    table.put(put);
    table.flushCommits();
    rm.putTable(table);
  }

  public boolean changePassword(String username, String oldPassword,
      String newPassword) throws IOException {
    HTable table = rm.getTable(UserTable.NAME);
    Put put = new Put(Bytes.toBytes(username));
    put.add(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
        Bytes.toBytes(newPassword));
    boolean check = table.checkAndPut(Bytes.toBytes(username),
        UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
        Bytes.toBytes(oldPassword), put);
    table.flushCommits();
    rm.putTable(table);
    return check;
  }

  public void adminChangePassword(String username, String newPassword)
      throws IOException {
    HTable table = rm.getTable(UserTable.NAME);
    Put put = new Put(Bytes.toBytes(username));
    put.add(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
        Bytes.toBytes(newPassword));
    table.put(put);
    table.flushCommits();
    rm.putTable(table);
  }

  public User getUser(String username) throws IOException {
    User user = null;
    HTable table = null;
    try {
      table = rm.getTable(UserTable.NAME);
      Get get = new Get(Bytes.toBytes(username));

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }

      String firstName = Bytes.toString(result.getValue(
          UserTable.DATA_FAMILY, UserTable.FIRSTNAME));
      String lastName = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.LASTNAME));
      String email = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.EMAIL));
      String credentials = Bytes.toString(result.getValue(
          UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
      String roles = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.ROLES));
      user = new User(username, firstName, lastName, email, credentials,
          roles);
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
          String email = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.EMAIL));
          String credentials = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
          String roles = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.ROLES));
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

  /**
   * Convenience method to retrieve a new anonymous User Id. Each call
   * increments the counter by one.
   * 
   * @return The newly created user Id.
   * @throws IOException When communicating with HBase fails.
   */
  public String generateAnonymousUserId() throws IOException {
    return generateAnonymousUserId(1L) + ANONYMOUS_SUFFIX;
  }

  /**
   * Creates a new short Id.
   * 
   * @param incrBy The increment value.
   * @return The newly created short id, encoded as String.
   * @throws IOException When the counter fails to increment.
   */
  private String generateAnonymousUserId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY,
          HushTable.ANONYMOUS_USER_ID, incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(HushTable.COUNTERS_FAMILY,
          HushTable.ANONYMOUS_USER_ID));
      return HushUtil.hushEncode(id);
    } catch (Exception e) {
      LOG.error("Unable to create a new anonymous user Id.", e);
      throw new IOException(e);
    } finally {
      try {
        manager.putTable(table);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  public static boolean isAnonymous(String username) {
    return username == null || username.endsWith(ANONYMOUS_SUFFIX);
  }
}
