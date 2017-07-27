package security;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.security.SecurityCapability;

import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import util.HBaseHelper;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

// cc AccessControlExample Example using the API to handle ACLs
public class AccessControlExample {

  private static TableName tableName;

  // vv AccessControlExample
  static class AuthenticatedUser implements AutoCloseable {

    private UserGroupInformation ugi;
    private Configuration conf;
    private Connection connection;

    public AuthenticatedUser(String user, String path)
      throws IOException, InterruptedException {
      ugi = loginUserWithKeyTab(user, path); // co AccessControlExample-01-LoginKeytab Login the user with a given keytab.
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          conf = HBaseConfiguration.create();
          connection = ConnectionFactory.createConnection(conf); // co AccessControlExample-02-CreateConn Create the connection in the context of the authorized user.
          return null;
        }
      });
    }

    private UserGroupInformation loginUserWithKeyTab(String user, String path)
        throws IOException {
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, path);
    }

    public Connection getConnection() {
      return connection;
    }
    /*...*/
    // ^^ AccessControlExample

    public Configuration getConfiguration() {
      return conf;
    }

    public UserGroupInformation getUgi() {
      return ugi;
    }

    public String getShortUserName() {
      return ugi.getShortUserName();
    }
    // vv AccessControlExample

    public <T> T doAs(PrivilegedAction<T> action) {
      return ugi.doAs(action);
    }

    public <T> T doAs(PrivilegedExceptionAction<T> action)
      throws IOException, InterruptedException {
      return ugi.doAs(action);
    }

    @Override
    public void close() throws Exception {
      if (connection != null)
        connection.close();
      connection = null;
    }
    /*...*/
    // ^^ AccessControlExample

    public void grant(final String user, final Permission.Action... action)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            AccessControlClient.grant(connection, user, action); // co AccessControlExample-03-GrantHelper Call the access control client method in the context of the authenticated user.
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }
    // vv AccessControlExample

    public void grant(final TableName tableName, final String user,
      final String family, final String qualifier,
      final Permission.Action... action)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            AccessControlClient.grant(connection, tableName, user,
              family != null ? Bytes.toBytes(family) : null,
              qualifier != null ? Bytes.toBytes(qualifier): null,
              action);
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }
    /*...*/
    // ^^ AccessControlExample

    public void grant(final String namespace, final String user,
      final String family, final String qualifier,
      final Permission.Action... action)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            AccessControlClient.grant(connection, namespace, user, action);
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }

    public void revoke(final String user, final Permission.Action... action)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            AccessControlClient.revoke(connection, user, action);
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }
    // vv AccessControlExample

    public void revoke(final TableName tableName, final String user,
      final String family, final String qualifier,
      final Permission.Action... action)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            AccessControlClient.revoke(connection, tableName, user,
              family != null ? Bytes.toBytes(family) : null,
              qualifier != null ? Bytes.toBytes(qualifier): null,
              action);
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }
    /*...*/
    // ^^ AccessControlExample

    public void revoke(final String namespace, final String user,
      final String family, final String qualifier,
      final Permission.Action... action)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            AccessControlClient.revoke(connection, namespace, user, action);
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }
    // vv AccessControlExample

    public List<UserPermission> getUserPermissions(final String tableRegex)
      throws Throwable {
      return doAs(new PrivilegedExceptionAction<List<UserPermission>>() {
        @Override
        public List<UserPermission> run() throws Exception {
          try {
            return AccessControlClient.getUserPermissions(connection, tableRegex);
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
        }
      });
    }
    /*...*/
    // ^^ AccessControlExample

    public void printUserPermissions(final String tableRegex)
      throws Exception {
      doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            List<UserPermission> ups = ups = AccessControlClient.
              getUserPermissions(connection, tableRegex);
            System.out.println("User permissions:");
            for (UserPermission perm : ups) {
              System.out.println("  " + perm);
            }
          } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
          }
          return null;
        }
      });
    }

    public void scan(final TableName tableName, final Scan scan) {
      doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          try {
            Table table = connection.getTable(tableName);
            ResultScanner resultScanner = table.getScanner(scan);
            System.out.println("Starting scan...");
            int rows = 0;
            for (Result result: resultScanner) {
              System.out.println(result);
              rows++;
            }
            System.out.println("Found " + rows + " rows.");
          } catch (Exception e) {
            System.out.println("Scan failed with: " + e);
          }
          return null;
        }
      });
    }
    // vv AccessControlExample
  }

  // ^^ AccessControlExample
  static Subject getSubject() throws Exception {
    LoginContext context = new LoginContext("", new Subject(), null,
      new javax.security.auth.login.Configuration() {
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
          Map<String, String> options = new HashMap<String, String>();
          options.put("useKeyTab", "false");
          options.put("storeKey", "false");
          options.put("doNotPrompt", "true");
          options.put("useTicketCache", "true");
          options.put("renewTGT", "true");
          options.put("refreshKrb5Config", "true");
          options.put("isInitiator", "true");
          String ticketCache = System.getenv("KRB5CCNAME");
          if (ticketCache != null) {
            options.put("ticketCache", ticketCache);
          }
          options.put("debug", "true");

          return new AppConfigurationEntry[]{
              new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  options)};
        }
      });
    context.login();
    return context.getSubject();
  }

  // vv AccessControlExample
  public static void main(String[] args) throws Throwable {
    final AuthenticatedUser superuser = new AuthenticatedUser( // co AccessControlExample-04-LoginUsers Login the three user roles: superuser, global admin, and application user.
      "hbase/master-1.hbase.book@HBASE.BOOK", "/tmp/hbase.keytab");
    AuthenticatedUser admin = new AuthenticatedUser(
      "hbasebook@HBASE.BOOK", "/tmp/hbasebook.keytab");
    AuthenticatedUser app1 = new AuthenticatedUser(
      "app1user1@HBASE.BOOK", "/tmp/app1user1.keytab");

    tableName = TableName.valueOf("testtable");
    // ^^ AccessControlExample

    System.out.println("Superuser: Preparing table and data...");
    superuser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1", "colfam2");

        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 100, 100, "colfam1", "colfam2");
        helper.close();
        return null;
      }
    });

    System.out.println("Superuser: Checking cluster settings...");
    // vv AccessControlExample
    superuser.doAs(new PrivilegedExceptionAction<Void>() { // co AccessControlExample-05-DoAsSuperuser Run the next commands as the superuser.
      @Override
      public Void run() throws Exception {
        Connection connection = superuser.getConnection(); // co AccessControlExample-06-GetConn Get dedicated connection for authenticated user.
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(tableName);

        List<SecurityCapability> sc = admin.getSecurityCapabilities(); // co AccessControlExample-07-ListCaps List the security capabilities as reported from the Master.
        for (SecurityCapability cap : sc) {
          System.out.println(cap);
        }

        System.out.println("Report AccessController features...");
        System.out.println("Access Controller Running: " +
          AccessControlClient.isAccessControllerRunning(connection)); // co AccessControlExample-08-PrintAccCtlOpts Report the features enabled regarding access control.
        System.out.println("Authorization Enabled: " +
          AccessControlClient.isAuthorizationEnabled(connection));
        System.out.println("Cell Authorization Enabled: " +
          AccessControlClient.isCellAuthorizationEnabled(connection));

        List<UserPermission> ups = null;
        try {
          ups = AccessControlClient.getUserPermissions(connection, ".*"); // co AccessControlExample-09-PrintPerms Print the current permissions.
          System.out.println("User permissions:");
          for (UserPermission perm : ups) {
            System.out.println("  " + perm);
          }
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
        table.close();
        return null;
      }
    });

    // ^^ AccessControlExample
    System.out.println("Superuser: Grant global admin to hbasebook...");
    // vv AccessControlExample
    superuser.grant(admin.getShortUserName(), Permission.Action.ADMIN);

    // ^^ AccessControlExample
    System.out.println("Admin & App1: Print permissions...");
    // vv AccessControlExample
    admin.printUserPermissions(null);
    app1.printUserPermissions(tableName.toString());
    // ^^ AccessControlExample
    System.out.println("Application: Attempting to scan table...");
    // vv AccessControlExample
    app1.scan(tableName, new Scan());

    // ^^ AccessControlExample
    System.out.println("Admin: Grant table access to application...");
    // vv AccessControlExample
    admin.grant(tableName, app1.getShortUserName(), "colfam1", "col-1",
      Permission.Action.READ);
    app1.printUserPermissions(tableName.toString());
    // ^^ AccessControlExample
    System.out.println("Application: Attempting to scan table again...");
    // vv AccessControlExample
    app1.scan(tableName, new Scan());
  }
}
