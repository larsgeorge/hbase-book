package com.hbasebook.hush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.model.ShortDomain;
import com.hbasebook.hush.table.LongDomainTable;
import com.hbasebook.hush.table.ShortDomainTable;

public class DomainManager {
  private final Log LOG = LogFactory.getLog(DomainManager.class);
  private final ResourceManager rm;
  private String defaultDomain = "localhost";

  /**
   * Package private constructor so only ResourceManager can instantiate.
   *
   * @param rm The reference to the resource manager.
   */
  DomainManager(ResourceManager rm) {
    this.rm = rm;
  }

  public void init() throws IOException {
    createDomains();
  }

  public void createDomains() throws IOException {
    LOG.info("Creating test domains.");
    addLongDomain("oreil.ly", "www.oreilly.com");
    addLongDomain("oreil.ly", "www2.oreilly.com");
    addLongDomain("oreil.ly", "www.orly.com");
    addLongDomain("oreil.ly", "oreilly.co.uk");
    addLongDomain("oreil.ly", "asdfasdfasdf.com");
    addLongDomain("hba.se", "hbasebook.com");
    addLongDomain("hba.se", "whatever.com");
    addLongDomain("hba.se", "seeya.com");
  }

  /**
   * Gets a list of {@link ShortDomain}.
   *
   * @return the list
   * @throws IOException
   */
  public List<ShortDomain> listShortDomains() throws IOException {
    Table table = null;

    List<ShortDomain> domains = new ArrayList<ShortDomain>();

    try {
      table = rm.getTable(ShortDomainTable.NAME);
      Scan scan = new Scan();
      scan.addFamily(ShortDomainTable.DOMAINS_FAMILY);

      ResultScanner results = table.getScanner(scan);
      for (Result result : results) {
        List<String> domainsList = new ArrayList<String>();
        String shortDomain = Bytes.toString(result.getRow());

        Map<byte[], byte[]> domainsMap = result
            .getFamilyMap(ShortDomainTable.DOMAINS_FAMILY);
        for (byte[] dom : domainsMap.keySet()) {
          domainsList.add(Bytes.toString(dom));
        }

        domains.add(new ShortDomain(shortDomain, domainsList));
      }
    } finally {
      rm.putTable(table);
    }
    return domains;
  }

  /**
   * Adds a short to long domain mapping.
   *
   * @param shortDomain
   * @param longDomain
   * @throws IOException
   */
  public void addLongDomain(String shortDomain, String longDomain)
      throws IOException {
    Table shortTable = rm.getTable(ShortDomainTable.NAME);
    Table longTable = rm.getTable(LongDomainTable.NAME);

    try {
      byte[] shortBytes = Bytes.toBytes(shortDomain);
      byte[] longBytes = Bytes.toBytes(longDomain);

      // first add to sdom
      Put shortPut = new Put(shortBytes);
      shortPut.addColumn(ShortDomainTable.DOMAINS_FAMILY, longBytes,
        Bytes.toBytes(System.currentTimeMillis()));
      shortTable.put(shortPut);

      // then add to ldom
      Put longPut = new Put(longBytes);
      longPut.addColumn(LongDomainTable.DATA_FAMILY,
        LongDomainTable.SHORT_DOMAIN, shortBytes);
      longTable.put(longPut);
    } finally {
      rm.putTable(shortTable);
      rm.putTable(longTable);
    }
  }

  /**
   * Deletes a long domain mapping.
   *
   * @param longDomain
   * @throws IOException
   */
  public void deleteLongDomain(String longDomain) throws IOException {
    Table shortTable = rm.getTable(ShortDomainTable.NAME);
    Table longTable = rm.getTable(LongDomainTable.NAME);

    try {
      byte[] longBytes = Bytes.toBytes(longDomain);
      Result result = longTable.get(new Get(longBytes));
      if (!result.isEmpty()) {
        byte[] shortBytes = result
          .getValue(LongDomainTable.DATA_FAMILY, LongDomainTable.SHORT_DOMAIN);
        Delete d = new Delete(shortBytes);
        d.addColumn(ShortDomainTable.DOMAINS_FAMILY, longBytes);
        shortTable.delete(d);
        longTable.delete(new Delete(longBytes));
      }
    } finally {
      rm.putTable(shortTable);
      rm.putTable(longTable);
    }
  }

  /**
   * Deletes a short domain and all its mappings.
   *
   * @param shortDomain
   * @throws IOException
   */
  public void deleteShortDomain(String shortDomain) throws IOException {
    Table shortTable = rm.getTable(ShortDomainTable.NAME);
    Table longTable = rm.getTable(LongDomainTable.NAME);

    try {
      byte[] shortBytes = Bytes.toBytes(shortDomain);
      Result result = longTable.get(new Get(shortBytes));
      if (!result.isEmpty()) {
        Map<byte[], byte[]> domainsMap = result.getFamilyMap(
          ShortDomainTable.DOMAINS_FAMILY);

        List<Delete> deletes = new ArrayList<Delete>();
        for (byte[] longDomain : domainsMap.keySet()) {
          deletes.add(new Delete(longDomain));
        }
        longTable.delete(deletes);
        shortTable.delete(new Delete(shortBytes));
      }
    } finally {
      rm.putTable(shortTable);
      rm.putTable(longTable);
    }
  }

  public void setDefaultDomain(String defaultDomain) {
    this.defaultDomain = defaultDomain;
  }

  public String getDefaultDomain() {
    return defaultDomain;
  }

  /**
   * Shortens a long domain.
   *
   * @param longDomain
   * @return The short domain mapped to longDomain, or defaultValue if no
   *         mapping exists.
   * @throws IOException
   */
  public String shorten(String longDomain) throws IOException {
    Table longTable = rm.getTable(LongDomainTable.NAME);

    try {
      Result result = longTable.get(new Get(Bytes.toBytes(longDomain)));
      if (!result.isEmpty()) {
        byte[] shortBytes = result.getValue(LongDomainTable.DATA_FAMILY,
          LongDomainTable.SHORT_DOMAIN);
        if (shortBytes != null && shortBytes.length != 0) {
          return Bytes.toString(shortBytes);
        }
      }
      return getDefaultDomain();
    } finally {
      rm.putTable(longTable);
    }
  }
}
