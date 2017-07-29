package security;

import java.util.List;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc VisibilityLabelExample Example that uses the HBase API to handle visibility labels.
public class VisibilityLabelExample {

  private static TableName tableName;

  // vv VisibilityLabelExample
  private static void addLabels(final AuthenticatedUser user,
    final String... labels) {
    user.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          VisibilityClient.addLabels(user.getConnection(), labels); // co VisibilityLabelExample-01-Helper Helper method to execute the method in the context of the authenticated user.
        } catch (Throwable throwable) {
          System.out.println("addLabels() failed with: " +
            throwable.getMessage().split("\n")[0]);
        }
        return null;
      }
    });
  }
  /*...*/
  // ^^ VisibilityLabelExample

  private static void printLabels(final AuthenticatedUser user,
    final String labelRegex) {
    user.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        VisibilityLabelsProtos.ListLabelsResponse response = null;
        try {
          response = VisibilityClient.listLabels(user.getConnection(),
            labelRegex);
          user.openConnection(); // See HBASE-18472
          System.out.println("Labels:");
          int count = 0;
          for (ByteString label : response.getLabelList()) {
            System.out.println("  " + label.toStringUtf8());
            count++;
          }
          System.out.println("Found " + count + " labels.");
        } catch (Throwable throwable) {
          System.out.println("printLabels() failed with: " +
            throwable.getMessage().split("\n")[0]);
        }
        return null;
      }
    });
  }
  // vv VisibilityLabelExample

  public static void setUserAuthorization(final AuthenticatedUser user,
    final String assignee, final String... labels) {
    user.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          VisibilityClient.setAuths(user.getConnection(), labels, assignee);
        } catch (Throwable throwable) {
          System.out.println("setUserAuthorization() failed with: " +
            throwable.getMessage().split("\n")[0]);
        }
        return null;
      }
    });
  }

  public static void removeUserAuthorization(final AuthenticatedUser user,
    final String assignee, final String... labels) {
    user.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          VisibilityClient.clearAuths(user.getConnection(), labels, assignee);
        } catch (Throwable throwable) {
          System.out.println("removeUserAuthorization() failed with: " +
            throwable.getMessage().split("\n")[0]);
        }
        return null;
      }
    });
  }

  public static void printUserAuthorization(final AuthenticatedUser user,
    final String assignee) {
    user.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          VisibilityLabelsProtos.GetAuthsResponse response =
            VisibilityClient.getAuths(user.getConnection(), assignee);
          System.out.println("User Authorizations for " + assignee + ":");
          int count = 0;
          for (ByteString auth : response.getAuthList()) {
            System.out.println("  " + auth.toStringUtf8());
            count++;
          }
          System.out.println("Got " + count + " user authorizations.");
        } catch (Throwable throwable) {
          System.out.println("printUserAuthorization() failed with: " +
            throwable.getMessage().split("\n")[0]);
        }
        return null;
      }
    });
  }

  public static void main(String[] args) throws Throwable {
    final AuthenticatedUser superuser = new AuthenticatedUser(
      "hbase/master-1.hbase.book@HBASE.BOOK", "/tmp/hbase.keytab");
    final AuthenticatedUser admin = new AuthenticatedUser(
      "hbasebook@HBASE.BOOK", "/tmp/hbasebook.keytab");
    final AuthenticatedUser app1 = new AuthenticatedUser(
      "app1user1@HBASE.BOOK", "/tmp/app1user1.keytab");

    tableName = TableName.valueOf("testtable");
    /*...*/
    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Preparing table and data...");
    superuser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Configuration conf = superuser.getConfiguration();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 2, 2, "colfam1");
        helper.close();
        return null;
      }
    });
    // vv VisibilityLabelExample

    System.out.println("Superuser: Checking cluster settings...");
    superuser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Connection connection = superuser.getConnection();
        Admin admin = connection.getAdmin();

        List<SecurityCapability> sc = admin.getSecurityCapabilities();
        System.out.println("Available security capabilities:");
        for (SecurityCapability cap : sc) {
          System.out.println("  " + cap);
        }
        admin.close();

        System.out.println("Report Visibility features...");
        System.out.println("  Visibility Controller Running: " +
          VisibilityClient.isCellVisibilityEnabled(connection)); // co VisibilityLabelExample-02-CheckStatus Determine if the visibility labels are enabled on the cluster.
        return null;
      }
    });

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Print all labels...");
    //printLabels(superuser, ".*");
    // vv VisibilityLabelExample

    superuser.grant(admin.getShortUserName(), Permission.Action.values()); // co VisibilityLabelExample-03-Grants Grant ACL access to the data or else all access is denied.
    admin.grant(tableName, app1.getShortUserName(), null, null,
      Permission.Action.values());

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Add labels...");
    // vv VisibilityLabelExample
    addLabels(superuser, "low", "medium", "high"); // co VisibilityLabelExample-04-AddLabels Add the system-wide labels.

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Print current labels...");
    // vv VisibilityLabelExample
    //printLabels(superuser, ".*");

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Add authorization \"low\" and " +
      "\"medium\" for admin...");
    // vv VisibilityLabelExample
    setUserAuthorization(superuser, admin.getShortUserName(), // co VisibilityLabelExample-05-AddAuth Assign the labels to each user, in the required combination.
      "low", "medium", "system");
    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Add authorization \"low\" for application...");
    // vv VisibilityLabelExample
    setUserAuthorization(superuser, app1.getShortUserName(), "low");

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Print user authorizations...");
    // vv VisibilityLabelExample
    printUserAuthorization(superuser, admin.getShortUserName());
    printUserAuthorization(superuser, app1.getShortUserName());

    // ^^ VisibilityLabelExample
    System.out.println("Admin: Print user authorizations...");
    // vv VisibilityLabelExample
    printUserAuthorization(admin, admin.getShortUserName()); // co VisibilityLabelExample-06-PrintAuth This and the next call will fail due to insufficient rights.
    printUserAuthorization(admin, app1.getShortUserName());

    // ^^ VisibilityLabelExample
    System.out.println("Application: Print user authorizations...");
    // vv VisibilityLabelExample
    printUserAuthorization(app1, app1.getShortUserName());

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Add a cell with \"medium AND high" +
      "\" visibility...");
    // vv VisibilityLabelExample
    Put put = new Put(Bytes.toBytes("row-98"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-medium-high"),
      Bytes.toBytes("medium and high visibility"));
    put.setCellVisibility(new CellVisibility("medium & high")); // co VisibilityLabelExample-07-AddCell Add new cells with specific cell visibility expressions set.
    superuser.put(tableName, put);

    // ^^ VisibilityLabelExample
    System.out.println("Admin: Add a cell with \"low OR medium\" visibility...");
    // vv VisibilityLabelExample
    put = new Put(Bytes.toBytes("row-99"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-low-medium"),
      Bytes.toBytes("low "));
    put.setCellVisibility(new CellVisibility("low | medium"));
    admin.put(tableName, put);

    // ^^ VisibilityLabelExample
    System.out.println("Admin: Scan table...");
    // vv VisibilityLabelExample
    admin.scan(tableName, new Scan()); // co VisibilityLabelExample-08-Scans Scan the table twice, as different users. The admin can see more than the application due to the visibility expressions.
    // ^^ VisibilityLabelExample
    System.out.println("Application: Scan table...");
    // vv VisibilityLabelExample
    app1.scan(tableName, new Scan());

    // ^^ VisibilityLabelExample
    System.out.println("Admin: Scan table again, but with reduced visbility...");
    // vv VisibilityLabelExample
    admin.scan(tableName,
      new Scan().setAuthorizations(new Authorizations("low")));
    admin.scan(tableName,
      new Scan().setAuthorizations(new Authorizations("high")));

    // ^^ VisibilityLabelExample
    System.out.println("Superuser: Remove labels from admin user and try scan again...");
    // vv VisibilityLabelExample
    removeUserAuthorization(superuser, admin.getShortUserName(), "medium");
    printUserAuthorization(superuser, admin.getShortUserName());
    admin.scan(tableName, new Scan());
  }
  // ^^ VisibilityLabelExample

}
