package com.hbasebook.hush;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.model.LongUrl;
import com.hbasebook.hush.model.ShortUrl;
import com.hbasebook.hush.servlet.RequestInfo;
import com.hbasebook.hush.table.HushTable;
import com.hbasebook.hush.table.LongUrlTable;
import com.hbasebook.hush.table.ShortUrlTable;
import com.hbasebook.hush.table.UserShortUrlTable;

public class UrlManager {
  private final Log LOG = LogFactory.getLog(UrlManager.class);
  private final ResourceManager rm;

  UrlManager(ResourceManager rm) throws IOException {
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
      byte[] value = Bytes.toBytes(HushUtil.hushDecode("0337"));
      put.add(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, value);
      boolean hasPut = table.checkAndPut(HushTable.GLOBAL_ROW_KEY,
          HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, null, put);
      if (hasPut) {
        LOG.info("Short Id counter initialized.");
      }
    } catch (Exception e) {
      LOG.error("Unable to initialize counters.", e);
      throw new IOException(e);
    } finally {
      rm.putTable(table);
    }
  }

  /**
   * Creates a new short URL entry. Details are stored in various tables.
   *
   * @param url The URL to shorten.
   * @param username The name of the current user.
   * @return The shortened URL.
   * @throws IOException When reading or writing the data fails.
   */
  public ShortUrl createShortUrl(URL url, String username, RequestInfo info)
      throws IOException {
    String shortId = generateShortId();
    String host = rm.getDomainManager().shorten(url.getHost());
    String urlString = url.toString();
    String refShortId = getReferenceShortId(urlString, shortId);

    ShortUrl shortUrl = null;
    if (refShortId != null && UserManager.isAnonymous(username)) {
      // no need to create a new link, just look up an existing one
      shortUrl = getShortUrl(refShortId);
      createUserShortUrlMapping(username, refShortId);
    } else {
      shortUrl = new ShortUrl(shortId, host, urlString, refShortId, username);
      createShortUrlMapping(shortUrl);
      createUserShortUrlMapping(username, shortId);
      rm.getCounters().incrementUsage(shortId, info, 0L);
    }
    return shortUrl;
  }

  /**
   * Returns the reference short Id of a given URL. The reference Id is the
   * first short Id created for a URL. If there is no entry yet a new one is
   * created, and it the reference Id for the new entry is the shortId
   * parameter.
   *
   * @param url The URL to look up.
   * @param shortId Assign this as the reference ID if the URL does not exist.
   * @return The shortId for the URL, if an entry exists, or null if a new entry
   *         was created.
   * @throws IOException When reading or writing the data fails.
   */
  private String getReferenceShortId(String url, String shortId)
      throws IOException {
    if (!addLongUrl(url, shortId)) {
      LongUrl longUrl = getLongUrl(url);
      return longUrl.getShortId();
    }
    return null;
  }

  /**
   * Adds a new long URL record to the table, but only if there is no previous
   * entry.
   *
   * @param longUrl The details of what to add.
   * @return <code>true</code> when the add has succeeded.
   * @throws IOException When reading or writing the data fails.
   */
  private boolean addLongUrl(String longUrl, String shortId)
      throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(LongUrlTable.NAME);
    byte[] md5Url = DigestUtils.md5(longUrl);
    Put put = new Put(md5Url);
    put.add(LongUrlTable.DATA_FAMILY, LongUrlTable.URL,
        Bytes.toBytes(longUrl));
    put.add(LongUrlTable.DATA_FAMILY, LongUrlTable.SHORT_ID,
        Bytes.toBytes(shortId));
    boolean hasPut = table.checkAndPut(md5Url, LongUrlTable.DATA_FAMILY,
        LongUrlTable.URL, null, put);
    manager.putTable(table);
    return hasPut;
  }

  /**
   * Saves a mapping between the short Id and long URL.
   *
   * @param shortUrl The short URL details.
   * @throws IOException When saving the record fails.
   */
  private void createShortUrlMapping(ShortUrl shortUrl) throws IOException {
    HTable table = rm.getTable(ShortUrlTable.NAME);
    Put put = new Put(Bytes.toBytes(shortUrl.getId()));
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL,
        Bytes.toBytes(shortUrl.getLongUrl()));
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN,
        Bytes.toBytes(shortUrl.getDomain()));
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID,
        Bytes.toBytes(shortUrl.getUser()));
    if (shortUrl.getRefShortId() != null) {
      put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID,
          Bytes.toBytes(shortUrl.getRefShortId()));
    }
    table.put(put);
    table.flushCommits();
    rm.putTable(table);
  }

  /**
   * Saves a mapping between the short Id and long URL.
   *
   * @param shortUrl The short URL details.
   * @throws IOException When saving the record fails.
   */
  private void createUserShortUrlMapping(String username, String shortId)
      throws IOException {
    HTable table = rm.getTable(UserShortUrlTable.NAME);
    byte[] rowKey = Bytes.add(Bytes.toBytes(username), ResourceManager.ZERO,
        Bytes.toBytes(shortId));
    Put put = new Put(rowKey);
    put.add(UserShortUrlTable.DATA_FAMILY, UserShortUrlTable.TIMESTAMP,
        Bytes.toBytes(System.currentTimeMillis()));
    // put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL,
    // Bytes.toBytes(shortUrl.getLongUrl()));
    // put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN,
    // Bytes.toBytes(shortUrl.getDomain()));
    // put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID,
    // Bytes.toBytes(shortUrl.getUser()));
    // if (shortUrl.getRefShortId() != null) {
    // put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID,
    // Bytes.toBytes(shortUrl.getRefShortId()));
    // }
    table.put(put);
    table.flushCommits();
    rm.putTable(table);
  }

  /**
   * Loads the details of a shortened URL by Id.
   *
   * @param shortId The Id to load.
   * @return The shortened URL details.
   * @throws IOException When reading the data fails.
   */
  public ShortUrl getShortUrl(String shortId) throws IOException {
    if (shortId == null) {
      return null;
    }

    HTable table = rm.getTable(ShortUrlTable.NAME);

    Get get = new Get(Bytes.toBytes(shortId));
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID);
    Result result = table.get(get);
    if (result.isEmpty()) {
      return null;
    }

    String url = Bytes.toString(result.getValue(ShortUrlTable.DATA_FAMILY,
        ShortUrlTable.URL));
    if (url == null) {
      LOG.warn("Found " + shortId + " but no URL column.");
      return null;
    }

    String domain = Bytes.toString(result.getValue(ShortUrlTable.DATA_FAMILY,
        ShortUrlTable.SHORT_DOMAIN));
    if (domain == null) {
      LOG.warn("Found " + shortId + " but no short domain column.");
      return null;
    }

    String refShortId = Bytes.toString(result.getValue(
        ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID));
    String user = Bytes.toString(result.getValue(ShortUrlTable.DATA_FAMILY,
        ShortUrlTable.USER_ID));

    rm.putTable(table);
    return new ShortUrl(shortId, domain, url, refShortId, user);
  }

  private LongUrl getLongUrl(String longUrl) throws IOException {
    HTable table = rm.getTable(LongUrlTable.NAME);

    byte[] md5Url = DigestUtils.md5(longUrl);
    Get get = new Get(md5Url);
    get.addColumn(LongUrlTable.DATA_FAMILY, LongUrlTable.URL);
    get.addColumn(LongUrlTable.DATA_FAMILY, LongUrlTable.SHORT_ID);
    Result result = table.get(get);
    if (result.isEmpty()) {
      return null;
    }

    String url = Bytes.toString(result.getValue(LongUrlTable.DATA_FAMILY,
        LongUrlTable.URL));
    String shortId = Bytes.toString(result.getValue(LongUrlTable.DATA_FAMILY,
        LongUrlTable.SHORT_ID));

    rm.putTable(table);
    return new LongUrl(url, shortId);
  }

  /**
   * Convenience method to retrieve a new short Id. Each call increments the
   * counter by one.
   *
   * @return The newly created short Id.
   * @throws IOException When communicating with HBase fails.
   */
  private String generateShortId() throws IOException {
    return generateShortId(1L);
  }

  /**
   * Creates a new short Id.
   *
   * @param incrBy The increment value.
   * @return The newly created short id, encoded as String.
   * @throws IOException When the counter fails to increment.
   */
  private String generateShortId(long incrBy) throws IOException {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(HushTable.NAME);
    try {
      Increment increment = new Increment(HushTable.GLOBAL_ROW_KEY);
      increment.addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID,
          incrBy);
      Result result = table.increment(increment);
      long id = Bytes.toLong(result.getValue(HushTable.COUNTERS_FAMILY,
          HushTable.SHORT_ID));
      return HushUtil.hushEncode(id);
    } catch (Exception e) {
      LOG.error("Unable to create a new short Id.", e);
      throw new IOException(e);
    } finally {
      try {
        manager.putTable(table);
      } catch (Exception e) {
        // ignore
      }
    }
  }
}
