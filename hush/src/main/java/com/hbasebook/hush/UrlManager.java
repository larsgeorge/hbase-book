package com.hbasebook.hush;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
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
    initializeShortIdCounter();
  }

  /**
   * Initializes the short ID counter
   *
   * @throws IOException
   */
  private void initializeShortIdCounter() throws IOException {
    Table table = rm.getTable(HushTable.NAME);
    try {
      Put put = new Put(HushTable.GLOBAL_ROW_KEY);
      byte[] value = Bytes.toBytes(HushUtil.hushDecode("0337"));
      put.addColumn(HushTable.COUNTERS_FAMILY, HushTable.SHORT_ID, value);
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
  public ShortUrl shorten(URL url, String username, RequestInfo info)
    throws IOException {
    String shortId = generateShortId();
    String domain = rm.getDomainManager().shorten(url.getHost());
    String urlString = url.toString();
    String refShortId = getReferenceShortId(domain, urlString, username);

    ShortUrl shortUrl;
    if (refShortId != null && UserManager.isAnonymous(username)) {
      // no need to create a new link, just look up an existing one
      shortUrl = getShortUrl(refShortId);
      createUserShortUrl(username, refShortId);
    } else {
      shortUrl = new ShortUrl(shortId, domain, urlString, refShortId, username);
      createShortUrl(shortUrl);
      createUserShortUrl(username, shortId);
      rm.getCounters().incrementUsage(shortId, info, 0L);
    }
    return shortUrl;
  }

  /**
   * Returns the reference short Id of a given URL. The reference Id is the
   * first short Id created for a URL. If there is no entry yet a new one is
   * created.
   *
   * @param domain  The domain name to use.
   * @param url  The long URL to look up.
   * @param username  The current username.
   * @return The reference shortId for the URL.
   * @throws IOException When reading or writing the data fails.
   */
  private String getReferenceShortId(String domain, String url, String username)
    throws IOException {
    String shortId = addLongUrl(domain, url, username);
    if (shortId == null) {
      LongUrl longUrl = getLongUrl(url);
      shortId = longUrl != null ? longUrl.getShortId() : null;
    }
    return shortId;
  }

  /**
   * Adds a new long URL record to the table, but only if there is no previous
   * entry.
   *
   * @param domain  The domain name to use.
   * @param url  The long URL to look up.
   * @param username  The current username.
   * @return The new short Id when the add has succeeded.
   * @throws IOException When adding the new URL fails.
   */
  private String addLongUrl(String domain, String url, String username)
    throws IOException {
    ResourceManager rm = ResourceManager.getInstance();
    Table table = rm.getTable(LongUrlTable.NAME);
    byte[] md5Url = DigestUtils.md5(url);
    Put put = new Put(md5Url);
    put.addColumn(LongUrlTable.DATA_FAMILY, LongUrlTable.URL,
      Bytes.toBytes(url));
    boolean hasPut = table.checkAndPut(md5Url, LongUrlTable.DATA_FAMILY,
      LongUrlTable.URL, null, put);
    String shortId = null;
    // check if we added a new URL, if so assign an Id subsequently
    if (hasPut) {
      shortId = generateShortId();
      createShortUrl(new ShortUrl(shortId, domain, url, null, username));
      put.addColumn(LongUrlTable.DATA_FAMILY, LongUrlTable.SHORT_ID,
        Bytes.toBytes(shortId));
      table.put(put);
    }
    rm.putTable(table);
    return shortId;
  }

  /**
   * Saves a mapping between the short Id and long URL.
   *
   * @param shortUrl The short URL details.
   * @throws IOException When saving the record fails.
   */
  private void createShortUrl(ShortUrl shortUrl) throws IOException {
    Table table = rm.getTable(ShortUrlTable.NAME);
    Put put = new Put(Bytes.toBytes(shortUrl.getId()));
    put.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL,
      Bytes.toBytes(shortUrl.getLongUrl()));
    put.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN,
      Bytes.toBytes(shortUrl.getDomain()));
    put.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID,
      Bytes.toBytes(shortUrl.getUser()));
    if (shortUrl.getRefShortId() != null) {
      put.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID,
        Bytes.toBytes(shortUrl.getRefShortId()));
    }
    put.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.CLICKS,
      Bytes.toBytes(shortUrl.getClicks()));

    table.put(put);
    rm.putTable(table);
  }

  /**
   * Saves a mapping between the short Id and long URL.
   *
   * @param username  The current username.
   * @param shortId  The short Id to kink the user to.
   * @throws IOException When saving the record fails.
   */
  private void createUserShortUrl(String username, String shortId)
    throws IOException {
    Table table = rm.getTable(UserShortUrlTable.NAME);
    byte[] rowKey = Bytes.add(Bytes.toBytes(username), ResourceManager.ZERO,
      Bytes.toBytes(shortId));
    Put put = new Put(rowKey);
    put.addColumn(UserShortUrlTable.DATA_FAMILY, UserShortUrlTable.TIMESTAMP,
      Bytes.toBytes(System.currentTimeMillis()));
    table.put(put);
    rm.putTable(table);
  }

  /**
   * Loads the details of a shortened URL by Id.
   *
   * @param shortId The Id to load.
   * @return The shortened URL details, or <code>null</code> if not found.
   * @throws IOException When reading the data fails.
   */
  public ShortUrl getShortUrl(String shortId) throws IOException {
    if (shortId == null) {
      return null;
    }

    Table table = rm.getTable(ShortUrlTable.NAME);

    Get get = new Get(Bytes.toBytes(shortId));
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.REF_SHORT_ID);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.CLICKS);
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
    long clicks = Bytes.toLong(result.getValue(ShortUrlTable.DATA_FAMILY,
      ShortUrlTable.CLICKS));

    rm.putTable(table);
    return new ShortUrl(shortId, domain, url, refShortId, user, clicks);
  }

  /**
   * Loads a URL from the URL table.
   *
   * @param longUrl  The URL to load.
   * @return The URL with its details, or <code>null</code> if not found.
   * @throws IOException When loading the URL fails.
   */
  private LongUrl getLongUrl(String longUrl) throws IOException {
    Table table = rm.getTable(LongUrlTable.NAME);

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
    ResourceManager rm = ResourceManager.getInstance();
    Table table = rm.getTable(HushTable.NAME);
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
      rm.putTable(table, true);
    }
  }

  private List<String> getShortUrlIdsByUser(String username)
    throws IOException {
    Table table = rm.getTable(UserShortUrlTable.NAME);

    byte[] startRow = Bytes.toBytes(username);
    byte[] stopRow = Bytes.add(startRow, ResourceManager.ONE);

    Scan scan = new Scan(startRow, stopRow);
    scan.addFamily(UserShortUrlTable.DATA_FAMILY);

    ResultScanner scanner = table.getScanner(scan);
    List<String> ids = new ArrayList<String>();
    for (Result result : scanner) {
      String rowKey = Bytes.toString(result.getRow());
      String shortId = rowKey.substring(rowKey.indexOf(0) + 1);
      ids.add(shortId);
    }
    rm.putTable(table);
    return ids;
  }

  public List<ShortUrl> getShortUrlsByUser(String username)
    throws IOException {
    List<ShortUrl> surls = new ArrayList<ShortUrl>();
    for (String id : getShortUrlIdsByUser(username)) {
      surls.add(getShortUrl(id));
    }
    return surls;
  }
}
