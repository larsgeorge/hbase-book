package com.hbasebook.hush;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.model.ShortUrl;
import com.hbasebook.hush.model.User;
import com.hbasebook.hush.servlet.RequestInfo;
import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.UserTable;

public class UserManager {
  private final Log LOG = LogFactory.getLog(UserManager.class);

  private static final String ANONYMOUS_SUFFIX = ":anon";
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final String ADMIN_LOGIN_STRING = "admin";
  private static final byte[] ADMIN_LOGIN = Bytes.toBytes(ADMIN_LOGIN_STRING);
  private static final byte[] ADMIN_PASSWORD = ADMIN_LOGIN;

  private final ResourceManager rm;

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
    createRootUser();
    initializeAnonUserCounter();
    initializeAdminStats();
  }

  /**
   * Creates the root user "admin" with root access, ie. all roles.
   *
   * @throws IOException When creating the user fails.
   */
  private void createRootUser() throws IOException {
    Table table = rm.getTable(UserTable.NAME);
    try {
      Put put = new Put(ADMIN_LOGIN);
      put.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
        ADMIN_PASSWORD);
      put.addColumn(UserTable.DATA_FAMILY, UserTable.ROLES,
        UserTable.ADMIN_ROLES);
      boolean hasPut = table.checkAndPut(ADMIN_LOGIN,
        UserTable.DATA_FAMILY, UserTable.ROLES, null, put);
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

  /**
   * Initializes the anonymous user ID counter.
   *
   * @throws IOException When initializing the counter fails.
   */
  private void initializeAnonUserCounter() throws IOException {
    Table table = rm.getTable(HushTable.NAME);
    try {
      Put put = new Put(HushTable.GLOBAL_ROW_KEY);
      put.addColumn(HushTable.COUNTERS_FAMILY, HushTable.ANONYMOUS_USER_ID,
        Bytes.toBytes(HushUtil.hushDecode("0")));
      boolean hasPut = table.checkAndPut(HushTable.GLOBAL_ROW_KEY,
        HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, null, put);
      if (hasPut) {
        LOG.info("Anonymous User Id counter initialized.");
      }
    } catch (Exception e) {
      LOG.error("Unable to initialize counters.", e);
      throw new IOException(e);
    } finally {
      rm.putTable(table);
    }
  }

  /**
   * Adds usage statistics for the admin user. This is mainly for testing.
   *
   * @throws IOException When adding the statistics fails.
   */
  private void initializeAdminStats() throws IOException {
    Map<RequestInfo.InfoName, String> props =
      new HashMap<RequestInfo.InfoName, String>();
    props.put(RequestInfo.InfoName.RemoteAddr, getRandomIp());
    RequestInfo info = new RequestInfo(props);
    ShortUrl shortUrl = rm.getUrlManager().shorten(
      new URL("http://hbasebook.com"), "admin", info);
    Calendar startDate = Calendar.getInstance();
    startDate.set(2011, Calendar.JANUARY, 1);
    Calendar endDate = Calendar.getInstance();
    endDate.setTime(new Date());
    while (startDate.before(endDate)) {
      props.put(RequestInfo.InfoName.RemoteAddr, getRandomIp());
      int count = RANDOM.nextInt(200);
      rm.getCounters().incrementUsage(shortUrl.getId(), info,
        count, startDate.getTime());
      if (shortUrl.getRefShortId() != null) {
        rm.getCounters().incrementUsage(shortUrl.getRefShortId(), info,
          count, startDate.getTime());
      }
      startDate.add(Calendar.DATE, 1);
    }
    LOG.info("Admin statistics initialized.");
  }

  /**
   * Defines some well-known IP ranges, up to the first three octets.
   */
  private final String[] IP_BLOCK_BY_COUNTRY = { "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "20.0.0", // usa
      "2.24.0", // uk
      "2.24.0", // uk
      "2.24.0", // uk
      "1.0.1", // china
      "91.142.133", // russian fed
      "91.142.133", // russian fed
      "91.142.133", // russian fed
      "91.142.133", // russian fed
      "192.80.208", // whatup bra
      "192.80.208", // whatup bra
      "91.198.2", // germany
      "198.49.164", // argentina
      "180.149.200", // jp
      "91.142.143", // it
      "131.150.6", // ca
  };

  /**
   * Returns a "random" IP address based on weighted blocks of IP ranges. The
   * idea is to not get roughly the same amount of hits per country but get
   * some more popular to those that have barely any hits. Clear, Eh?
   *
   * @return A randomly generated IP observing locality.
   */
  private String getRandomIp() {
    return String.format("%s.%d",
      IP_BLOCK_BY_COUNTRY[RANDOM.nextInt(IP_BLOCK_BY_COUNTRY.length)],
      RANDOM.nextInt(255));
  }

  /**
   * Creates a user in the user table.
   *
   * @param username The username to use.
   * @param firstName The first name of the user.
   * @param lastName The last name of the user.
   * @param email The email address of the user.
   * @param password The password of the user.
   * @param roles The user roles assigned to the new user.
   * @throws IOException When adding the user fails.
   * todo: Fix since no pool anymore
   */
  // cc HushHTablePoolUsage Using the pool in application code
  // vv HushHTablePoolUsage
  public void createUser(String username, String firstName, String lastName,
    String email, String password, String roles) throws IOException {
    /*[*/Table table = rm.getTable(UserTable.NAME);/*]*/
    Put put = new Put(Bytes.toBytes(username));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.FIRSTNAME,
      Bytes.toBytes(firstName));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.LASTNAME,
      Bytes.toBytes(lastName));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.EMAIL,
      Bytes.toBytes(email));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
      Bytes.toBytes(password));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.ROLES,
      Bytes.toBytes(roles));
    table.put(put);
    /*[*/rm.putTable(table);/*]*/
  }
  // ^^ HushHTablePoolUsage

  /**
   * Updates a user record.
   *
   * @param username The username to modify.
   * @param firstName The new first name.
   * @param lastName The new last name.
   * @param email The new email address.
   * @throws IOException When modifying the user fails.
   */
  public void updateUser(String username, String firstName, String lastName,
    String email) throws IOException {
    Table table = rm.getTable(UserTable.NAME);
    Put put = new Put(Bytes.toBytes(username));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.FIRSTNAME,
      Bytes.toBytes(firstName));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.LASTNAME,
      Bytes.toBytes(lastName));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.EMAIL,
      Bytes.toBytes(email));
    table.put(put);
    rm.putTable(table);
  }

  public boolean changePassword(String username, String oldPassword,
    String newPassword) throws IOException {
    Table table = rm.getTable(UserTable.NAME);
    Put put = new Put(Bytes.toBytes(username));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
      Bytes.toBytes(newPassword));
    boolean check = table.checkAndPut(Bytes.toBytes(username),
      UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
      Bytes.toBytes(oldPassword), put);
    rm.putTable(table);
    return check;
  }

  public void adminChangePassword(String username, String newPassword)
    throws IOException {
    Table table = rm.getTable(UserTable.NAME);
    Put put = new Put(Bytes.toBytes(username));
    put.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
      Bytes.toBytes(newPassword));
    table.put(put);
    rm.putTable(table);
  }

  public User getUser(String username) throws IOException {
    User user = null;
    Table table = null;
    try {
      table = rm.getTable(UserTable.NAME);
      Get get = new Get(Bytes.toBytes(username));

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }

      String firstName = Bytes.toString(
        result.getValue(UserTable.DATA_FAMILY, UserTable.FIRSTNAME));
      String lastName = Bytes.toString(
        result.getValue(UserTable.DATA_FAMILY, UserTable.LASTNAME));
      String email = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
        UserTable.EMAIL));
      String credentials = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
        UserTable.CREDENTIALS));
      String roles = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
        UserTable.ROLES));
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
    Table table = rm.getTable(UserTable.NAME);

    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);

    Iterator<Result> results = scanner.iterator();
    int errors = 0;
    while (results.hasNext()) {
      Result result = results.next();
      if (!result.isEmpty()) {
        try {
          String username = Bytes.toString(result.getRow());
          String firstName = Bytes.toString(
            result.getValue(UserTable.DATA_FAMILY, UserTable.FIRSTNAME));
          String lastName = Bytes.toString(
            result.getValue(UserTable.DATA_FAMILY, UserTable.LASTNAME));
          String email = Bytes.toString(
            result.getValue(UserTable.DATA_FAMILY, UserTable.EMAIL));
          String credentials = Bytes.toString(
            result.getValue(UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
          String roles = Bytes.toString(
            result.getValue(UserTable.DATA_FAMILY, UserTable.ROLES));
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
    ResourceManager rm = ResourceManager.getInstance();
    Table table = rm.getTable(HushTable.NAME);
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
      rm.putTable(table, true);
    }
  }

  public static boolean isAnonymous(String username) {
    return username == null || username.endsWith(ANONYMOUS_SUFFIX);
  }
}
