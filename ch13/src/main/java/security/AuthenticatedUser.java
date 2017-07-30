package security;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;

// cc AuthenticatedUser Convenience class that wraps authenticated users and their connections.
// vv AuthenticatedUser
public class AuthenticatedUser implements AutoCloseable { // co AuthenticatedUser-01-Class Dedicated class to handle separate connections for authenticated users.

  private UserGroupInformation ugi;
  private Configuration conf;
  private Connection connection;
  private String name;

  public AuthenticatedUser(String user, String path, String name)
    throws Exception {
    this.name = name;
    ugi = loginUserWithKeyTab(user, path); // co AuthenticatedUser-02-LoginKeytab Log in the user with a given keytab.
    openConnection();
  }

  private UserGroupInformation loginUserWithKeyTab(String user, String path)
    throws IOException {
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, path);
  }

  // Should be private, but public until HBASE-18473 is fixed. lg
  public void openConnection() throws Exception {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf); // co AuthenticatedUser-03-CreateConn Create the connection in the context of the authorized user.
        return null;
      }
    });
  }

  public Connection getConnection() {
    return connection;
  }
  /*...*/
  // ^^ AuthenticatedUser

  public Configuration getConfiguration() {
    return conf;
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }

  public String getShortUserName() {
    return ugi.getShortUserName();
  }
  // vv AuthenticatedUser

  public <T> T doAs(PrivilegedAction<T> action) { // co AuthenticatedUser-04-doAsHelper Convenience methods to execute priviledged calls.
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
  // ^^ AuthenticatedUser

  public void grant(final String user, final Permission.Action... action)
    throws Exception {
    doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          AccessControlClient.grant(connection, user, action);
          System.out.println(name + ": Granted " + new Permission(action) +
            " to " + user);
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
        return null;
      }
    });
  }
  // vv AuthenticatedUser

  public void grant(final TableName tableName, final String user,  // co AuthenticatedUser-05-GrantHelper Call the access control client method in the context of the authenticated user.
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
          System.out.println(name + ": Granted " + new Permission(action) +
            " to " + user);
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
        return null;
      }
    });
  }

  public void grant(final TableName tableName, final String user,  // co AuthenticatedUser-06-GrantCells Helper method that allows to grant permissions to cells.
    final Scan scan, final Permission.Action... action)
    throws Exception {
    doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan);
        Map<String, Permission> perms = new HashMap<>();
        perms.put(user, new Permission(action));
        int rows = 0, cells = 0;
        for (Result row : scanner) {
          for (Cell cell : row.listCells()) {
            Put put = new Put(cell.getRowArray(), cell.getRowOffset(),  // co AuthenticatedUser-07-GrantCellsLoop Iterate over all existing cells and add given ACLs, copy the cells as-is otherwise.
              cell.getRowLength());
            put.add(cell);
            put.setACL(perms);
            table.put(put); // put per cell to avoid possible OOMEs
            System.out.println("Put: " + put);
            cells++;
          }
          rows++;
        }
        System.out.println("Processed " + rows + " rows and " +
          cells + " cells.");
        System.out.println(name + ": Granted " + new Permission(action) +
          " to " + user);
        return null;
      }
    });
  }
  /*...*/
  // ^^ AuthenticatedUser

  public void grant(final String namespace, final String user,
    final String family, final String qualifier,
    final Permission.Action... action)
    throws Exception {
    doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          AccessControlClient.grant(connection, namespace, user, action);
          System.out.println(name + ": Granted " + new Permission(action) +
            " to " + user);
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
          System.out.println(name + ": Revoked " + new Permission(action)
            + " to " + user);
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
        return null;
      }
    });
  }
  // vv AuthenticatedUser

  public void revoke(final TableName tableName, final String user,  // co AuthenticatedUser-08-Revoke Helper method to revoke previously granted permissions.
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
          System.out.println(name + ": Revoked " + new Permission(action) +
            " to " + user);
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
        return null;
      }
    });
  }
  /*...*/
  // ^^ AuthenticatedUser

  public void revoke(final String namespace, final String user,
    final String family, final String qualifier,
    final Permission.Action... action)
    throws Exception {
    doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          AccessControlClient.revoke(connection, namespace, user, action);
          System.out.println(name + ": Revoked " + new Permission(action) +
            " to " + user);
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
        return null;
      }
    });
  }
  // vv AuthenticatedUser

  public List<UserPermission> getUserPermissions(final String tableRegex)  // co AuthenticatedUser-09-GetPerms Returns the permissions assigned to the matching tables. Only shows what applies to the current user (and may return an empty list).
    throws Throwable {
    return doAs(new PrivilegedExceptionAction<List<UserPermission>>() {
      @Override
      public List<UserPermission> run() throws Exception {
        try {
          return AccessControlClient.getUserPermissions(connection,
            tableRegex);
        } catch (Throwable throwable) {
          throw new RuntimeException(throwable);
        }
      }
    });
  }
  /*...*/
  // ^^ AuthenticatedUser

  public void printUserPermissions(final String tableRegex)
    throws Exception {
    doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          List<UserPermission> ups = ups = AccessControlClient.
            getUserPermissions(connection, tableRegex);
          System.out.println(name + ": User permissions (" +
            (tableRegex != null ? tableRegex : "hbase:acl") + "):");
          int count = 0;
          for (UserPermission perm : ups) {
            System.out.println("  " + perm);
            count++;
          }
          System.out.println("Found " + count + " permissions.");
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
          System.out.println(name + ": Starting scan...");
          int rows = 0;
          for (Result result: resultScanner) {
            if (result.isEmpty()) continue;
            for (Cell cell : result.listCells()) {
              System.out.println("  " + cell + " -> " +
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                  cell.getValueLength()));
            }
            rows++;
          }
          System.out.println("Found " + rows + " rows.");
          resultScanner.close();
          table.close();
        } catch (Exception e) {
          System.out.println("Scan failed with: " +
            (e != null && e.getMessage() != null ?
              e.getMessage().split("\n")[0] : e));
        }
        return null;
      }
    });
  }

  public void put(final TableName tableName, final Put put)
    throws Exception {
    doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Table table = connection.getTable(tableName);
        try {
          table.put(put);
          System.out.println(name + ": Put data into " + tableName);
        } catch(Exception e) {
          System.out.println("Put failed with: " +
            (e != null && e.getMessage() != null ?
              e.getMessage().split("\n")[0] : e));
        }
        return null;
      }
    });
  }

  public Result get(final TableName tableName, final Get get)
    throws Exception {
    return doAs(new PrivilegedExceptionAction<Result>() {
      @Override
      public Result run() throws Exception {
        Table table = connection.getTable(tableName);
        try {
          Result result = table.get(get);
          System.out.println(name + ": Get result:");
          if (!result.isEmpty()) {
            for (Cell cell : result.listCells()) {
              System.out.println("  " + cell + " -> " +
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                  cell.getValueLength()));
            }
          }
          return result;
        } catch(Exception e) {
          System.out.println("Get failed with: " +
            (e != null && e.getMessage() != null ?
              e.getMessage().split("\n")[0] : e));
        }
        return null;
      }
    });
  }
  // vv AuthenticatedUser
}
// ^^ AuthenticatedUser
