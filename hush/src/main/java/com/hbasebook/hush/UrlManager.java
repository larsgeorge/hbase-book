package com.hbasebook.hush;

import java.io.IOException;
import java.net.URL;

import com.hbasebook.hush.table.LongUrl;
import com.hbasebook.hush.table.LongUrlTable;
import com.hbasebook.hush.table.UserShortUrlTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.table.ShortUrl;
import com.hbasebook.hush.table.ShortUrlTable;

public class UrlManager {
  private final Log LOG = LogFactory.getLog(UrlManager.class);
  private final ResourceManager rm;

  UrlManager(ResourceManager rm) throws IOException {
    this.rm = rm;
  }

  /**
   * Creates a new short URL entry. Details are stored in various
   * tables.
   *
   * @param url  The URL to shorten.
   * @param username  The name of the current user.
   * @return The shortened URL.
   * @throws IOException When reading or writing the data fails.
   */
  public ShortUrl createShortUrl(URL url, String username)
    throws IOException {
    String host = rm.getDomainManager().shorten(url.getHost());
    byte[] shortId = rm.getCounters().getShortId();

    String shortIdString = Bytes.toString(shortId);
    String urlString = url.toString();
    String refShortId = getReferenceShortId(urlString, shortIdString,
      username);
    createUrlMapping(shortId, host, urlString, refShortId, username);
    rm.getCounters().incrementUsage(Bytes.toBytes(username), shortId);
    return new ShortUrl(Bytes.toString(shortId), host, urlString,
      refShortId, username);
  }

  /**
   * Returns the reference short Id of a given URL. The reference
   * Id is the first short Id created for a URL. If there is no
   * entry yet a new one is created.
   *
   * @param url  The URL to look up.
   * @param shortId  The new short Id.
   * @param username  The user name.
   * @return The reference short Id.
   * @throws IOException When reading or writing the data fails.
   */
  private String getReferenceShortId(String url, String shortId, String
    username) throws IOException {
    LongUrl longUrl = getLongUrl(url);
    if (longUrl == null) {
      longUrl = new LongUrl(url, shortId, username);
      boolean done = addLongUrl(longUrl);
      // there is now an entry, retrieve instead
      if (!done) {
        longUrl = getLongUrl(url);
      }
    }
    return longUrl.getShortId();
  }

  /**
   * Adds a new long URL record to the table, but only if there
   * is no previous entry.
   *
   * @param longUrl The details of what to add.
   * @return <code>true</code> when the add has succeeded.
   * @throws IOException When reading or writing the data fails.
   */
  public boolean addLongUrl(LongUrl longUrl) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(LongUrlTable.NAME);
    String md5Url = longUrl.getUrl(); // todo fixme !!!
    byte[] md5UrlBytes = Bytes.toBytes(md5Url);
    Put put = new Put(md5UrlBytes);
    put.add(LongUrlTable.DATA_FAMILY, LongUrlTable.URL,
      Bytes.toBytes(longUrl.getUrl()));
    put.add(LongUrlTable.DATA_FAMILY, LongUrlTable.SHORT_ID,
      Bytes.toBytes(longUrl.getShortId()));
    put.add(LongUrlTable.DATA_FAMILY, LongUrlTable.USER_ID,
      Bytes.toBytes(longUrl.getUser()));
    boolean hasPut = table.checkAndPut(md5UrlBytes,
      LongUrlTable.DATA_FAMILY, LongUrlTable.URL, null, put);
    manager.putTable(table);
    return hasPut;
  }

  private void createUrlMapping(byte[] shortId, String sdom,
    String longUrl, String refShortId, String username)
    throws IOException {
    HTable table = rm.getTable(ShortUrlTable.NAME);
    Put put = new Put(shortId);
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL,
      Bytes.toBytes(longUrl));
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN,
      Bytes.toBytes(sdom));
    if (refShortId != null) {
      put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID,
        Bytes.toBytes(refShortId));
    }
    if (username != null) {
      put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID,
        Bytes.toBytes(username));
    }
    table.put(put);
    table.flushCommits();
    rm.putTable(table);
  }

  /**
   * Loads the details of a shortened URL by Id.
   *
   * @param shortId  The Id to load.
   * @return The shortened URL details.
   * @throws IOException When reading the data fails.
   */
  public ShortUrl getShortUrl(String shortId) throws IOException {
    HTable table = rm.getTable(ShortUrlTable.NAME);

    Get get = new Get(Bytes.toBytes(shortId));
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL);
    get.addColumn(ShortUrlTable.DATA_FAMILY,
      ShortUrlTable.SHORT_DOMAIN);
    get.addColumn(ShortUrlTable.DATA_FAMILY,
      ShortUrlTable.REF_SHORT_ID);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID);
    Result result = table.get(get);
    if (result.isEmpty()) {
      return null;
    }

    String url = Bytes.toString(
      result.getValue(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL));
    if (url == null) {
      LOG.warn("Found " + shortId + " but no URL column.");
      return null;
    }

    String domain = Bytes.toString(
      result.getValue(ShortUrlTable.DATA_FAMILY,
        ShortUrlTable.SHORT_DOMAIN));
    if (domain == null) {
      LOG.warn("Found " + shortId + " but no short domain column.");
      return null;
    }

    String refShortId = Bytes.toString(
      result.getValue(ShortUrlTable.DATA_FAMILY,
        ShortUrlTable.REF_SHORT_ID));
    String user = Bytes.toString(
      result.getValue(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID)
    );

    rm.putTable(table);
    return new ShortUrl(shortId, domain, url, refShortId, user);
  }

  public LongUrl getLongUrl(String longUrl) throws IOException {
    HTable table = rm.getTable(LongUrlTable.NAME);

    String md5Url = longUrl; // todo fox me!!!
    Get get = new Get(Bytes.toBytes(md5Url));
    get.addColumn(LongUrlTable.DATA_FAMILY, LongUrlTable.URL);
    get.addColumn(LongUrlTable.DATA_FAMILY,
      LongUrlTable.SHORT_ID);
    get.addColumn(LongUrlTable.DATA_FAMILY, LongUrlTable.USER_ID);
    Result result = table.get(get);
    if (result.isEmpty()) {
      return null;
    }

    String url = Bytes.toString(
      result.getValue(LongUrlTable.DATA_FAMILY, LongUrlTable.URL));
    String shortId = Bytes.toString(
      result.getValue(LongUrlTable.DATA_FAMILY, LongUrlTable.SHORT_ID));
    String user = Bytes.toString(
      result.getValue(LongUrlTable.DATA_FAMILY, LongUrlTable.USER_ID));

    rm.putTable(table);
    return new LongUrl(url, shortId, user);
  }
}
