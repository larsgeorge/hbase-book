package com.hbasebook.hush;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.http.security.Credential;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.server.UserIdentity;

import com.hbasebook.hush.table.UserTable;

public class HBaseLoginService extends MappedLoginService {
  private final Log LOG = LogFactory.getLog(HBaseLoginService.class);

  private Configuration configuration = null;

  public HBaseLoginService(String name, Configuration configuration) {
    super();
    setName(name);
    this.configuration = configuration;
  }

  @Override
  protected UserIdentity loadUser(String username) {
    HTablePool pool;
    try {
      pool = ResourceManager.getInstance(configuration).getTablePool();
    } catch (IOException e) {
      LOG.error(String.format("Unable to get user '%s'", username), e);
      return null;
    }

    HTableInterface table = pool.getTable(UserTable.NAME);
    try {

      Get get = new Get(Bytes.toBytes(username));
      get.addColumn(UserTable.DATA_FAMILY, UserTable.CREDENTIALS);
      get.addColumn(UserTable.DATA_FAMILY, UserTable.ROLES);

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }

      String credentials = Bytes.toString(result.getValue(
          UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
      String roles = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
          UserTable.ROLES));

      return putUser(username, Credential.getCredential(credentials), roles
          .split(","));

    } catch (Exception e) {
      LOG.error(String.format("Unable to get user '%s'", username), e);
      return null;
    } finally {
      if (table != null) {
        pool.putTable(table);
      }
    }
  }

  @Override
  protected void loadUsers() throws IOException {
    HTablePool pool = ResourceManager.getInstance(configuration).getTablePool();
    HTableInterface table = pool.getTable(UserTable.NAME);

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
          String credentials = Bytes.toString(result.getValue(
              UserTable.DATA_FAMILY, UserTable.CREDENTIALS));
          String roles = Bytes.toString(result.getValue(UserTable.DATA_FAMILY,
              UserTable.ROLES));
          putUser(username, Credential.getCredential(credentials), roles
              .split(","));
        } catch (Exception e) {
          errors++;
        }
      }
    }
    if (errors > 0) {
      LOG.error(String.format("Encountered %d errors in loadUser", errors));
    }

    if (table != null) {
      pool.putTable(table);
    }
  }
}
