package com.hbasebook.hush;

import java.io.IOException;
import java.net.URL;

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

  private void createUrlMapping(byte[] shortId, String sdom, String longUrl,
      String username) throws IOException {
    HTable table = rm.getTable(ShortUrlTable.NAME);

    Put put = new Put(shortId);
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL, Bytes
        .toBytes(longUrl));
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN, Bytes
        .toBytes(sdom));
    if (username != null) {
      put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID, Bytes
          .toBytes(username));
    }
    table.put(put);
    table.flushCommits();
    rm.putTable(table);
  }

  public ShortUrl createShortUrl(URL url, String username) throws IOException {
    String host = rm.getDomainManager().shorten(url.getHost());
    byte[] shortId = rm.getCounters().getShortId();

    String urlString = url.toString();
    createUrlMapping(shortId, host, urlString, username);
    return new ShortUrl(Bytes.toString(shortId), host, urlString, username);
  }

  public ShortUrl getShortUrl(String shortId) throws IOException {
    HTable table = rm.getTable(ShortUrlTable.NAME);

    Get get = new Get(Bytes.toBytes(shortId));
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL);
    get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.SHORT_DOMAIN);
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

    String user = Bytes.toString(result.getValue(ShortUrlTable.DATA_FAMILY,
        ShortUrlTable.USER_ID));

    rm.putTable(table);
    return new ShortUrl(shortId, domain, url, user);
  }
}
