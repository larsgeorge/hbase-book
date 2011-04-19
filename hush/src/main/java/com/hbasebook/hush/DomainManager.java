package com.hbasebook.hush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.table.LongDomainTable;
import com.hbasebook.hush.table.ShortDomain;
import com.hbasebook.hush.table.ShortDomainTable;

public class DomainManager {
  private final Log LOG = LogFactory.getLog(DomainManager.class);
  private final ResourceManager rm;

  public DomainManager(ResourceManager rm) throws IOException {
    this.rm = rm;
  }

  public void createTestData() throws IOException {
    addLongDomain("oreil.ly", "www.oreilly.com");
    addLongDomain("oreil.ly", "www2.oreilly.com");
    addLongDomain("oreil.ly", "www.orly.com");
    addLongDomain("oreil.ly", "oreilly.co.uk");
    addLongDomain("oreil.ly", "asdfasdfasdf.com");
    addLongDomain("hba.se", "hbasebook.com");
    addLongDomain("hba.se", "whatever.com");
    addLongDomain("hba.se", "seeya.com");
  }

  public List<ShortDomain> listShortDomains() throws IOException {
    HTable table = null;

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
      if (table != null) {
        rm.putTable(table);
      }
    }
    return domains;
  }

  public void addLongDomain(String shortDomain, String longDomain)
      throws IOException {
    HTable shortTable = rm.getTable(ShortDomainTable.NAME);
    HTable longTable = rm.getTable(LongDomainTable.NAME);

    try {
      byte[] shortBytes = Bytes.toBytes(shortDomain);
      byte[] longBytes = Bytes.toBytes(longDomain);

      // first add to sdom
      Put shortPut = new Put(shortBytes);
      shortPut.add(ShortDomainTable.DOMAINS_FAMILY, longBytes, Bytes
          .toBytes(System.currentTimeMillis()));
      shortTable.put(shortPut);

      // then add to ldom
      Put longPut = new Put(longBytes);
      longPut.add(LongDomainTable.DATA_FAMILY, LongDomainTable.SHORT_DOMAIN,
          shortBytes);
    } finally {
      if (shortTable != null) {
        rm.putTable(shortTable);
      }
      if (longTable != null) {
        rm.putTable(longTable);
      }
    }
  }

  public void deleteLongDomain(String longDomain) throws IOException {
    HTable shortTable = rm.getTable(ShortDomainTable.NAME);
    HTable longTable = rm.getTable(LongDomainTable.NAME);

    try {
      byte[] longBytes = Bytes.toBytes(longDomain);
      Result result = longTable.get(new Get(longBytes));
      if (!result.isEmpty()) {
        byte[] shortBytes = result.getValue(LongDomainTable.DATA_FAMILY,
            LongDomainTable.SHORT_DOMAIN);

        longTable.delete(new Delete(longBytes));
        shortTable.delete(new Delete(shortBytes));
      }
    } finally {
      if (shortTable != null) {
        rm.putTable(shortTable);
      }
      if (longTable != null) {
        rm.putTable(longTable);
      }
    }
  }

  public void deleteShortDomain(String shortDomain) throws IOException {
    HTable shortTable = rm.getTable(ShortDomainTable.NAME);
    HTable longTable = rm.getTable(LongDomainTable.NAME);

    try {
      byte[] shortBytes = Bytes.toBytes(shortDomain);
      Result result = longTable.get(new Get(shortBytes));
      if (!result.isEmpty()) {
        Map<byte[], byte[]> domainsMap = result
            .getFamilyMap(ShortDomainTable.DOMAINS_FAMILY);

        List<Delete> deletes = new ArrayList<Delete>();
        for (byte[] longDomain : domainsMap.keySet()) {
          deletes.add(new Delete(longDomain));
        }
        longTable.delete(deletes);
        shortTable.delete(new Delete(shortBytes));
      }
    } finally {
      if (shortTable != null) {
        rm.putTable(shortTable);
      }
      if (longTable != null) {
        rm.putTable(longTable);
      }
    }
  }

  public String shorten(String longDomain, String defaultValue)
      throws IOException {
    HTable longTable = rm.getTable(LongDomainTable.NAME);

    try {
      Result result = longTable.get(new Get(Bytes.toBytes(longDomain)));
      if (!result.isEmpty()) {
        byte[] shortBytes = result.getValue(LongDomainTable.DATA_FAMILY,
            LongDomainTable.SHORT_DOMAIN);
        if (shortBytes != null && shortBytes.length != 0) {
          return Bytes.toString(shortBytes);
        }
      }
      return defaultValue;
    } finally {
      if (longTable != null) {
        rm.putTable(longTable);
      }
    }
  }

}
