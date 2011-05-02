package com.hbasebook.hush.schema;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This class provides support for XML based schemas. It handles creation and
 * alteration of tables based on external definition files.
 */
public class SchemaManager {
  private static final Log LOG = LogFactory.getLog(SchemaManager.class);

  private Configuration conf = null;
  private HBaseAdmin hbaseAdmin;
  private XMLConfiguration config = null;
  private ArrayList<TableSchema> schemas = null;
  private HTableDescriptor[] remoteTables = null;

  public SchemaManager(Configuration conf, String schemaName)
    throws ParseException, ConfigurationException {
    this.conf = conf;
    readConfiguration(schemaName);
  }

  private void readConfiguration(String schemaName)
    throws ConfigurationException {
    URL schemaUrl = Thread.currentThread().getContextClassLoader().
      getResource(schemaName);
    config = new XMLConfiguration(schemaUrl);
    schemas = new ArrayList<TableSchema>();
    readTableSchemas();
  }

  private void readTableSchemas() {
    final int maxTables = config.getMaxIndex("schema.table");
    for (int t = 0; t <= maxTables; t++) {
      final String base = "schema.table(" + t + ").";
      final TableSchema ts = new TableSchema();
      ts.setName(config.getString(base + "name"));
      if (config.containsKey(base + "description")) {
        ts.setDescription(config.getString(base + "description"));
      }
      if (config.containsKey(base + "deferred_log_flush")) {
        ts.setDeferredLogFlush(config.getBoolean(base + "deferred_log_flush"));
      }
      if (config.containsKey(base + "max_file_size")) {
        ts.setMaxFileSize(config.getLong(base + "max_file_size"));
      }
      if (config.containsKey(base + "memstore_flush_size")) {
        ts.setMemStoreFlushSize(config.getLong(base + "memstore_flush_size"));
      }
      if (config.containsKey(base + "read_only")) {
        ts.setReadOnly(config.getBoolean(base + "read_only"));
      }
      int idx = 0;
      while (true) {
        String kvKey = base + "key_value" + (idx++ == 0 ? "" : idx);
        if (config.containsKey(kvKey)) {
          String[] kv = config.getString(kvKey).split("=", 2);
          ts.addKeyValue(kv[0], kv.length > 1 ? kv[1] : null);
        } else {
          break;
        }
      }
      final int maxCols = config.getMaxIndex(base + "column_family");
      for (int c = 0; c <= maxCols; c++) {
        final String base2 = base + "column_family(" + c + ").";
        final ColumnDefinition cd = new ColumnDefinition();
        cd.setName(config.getString(base2 + "name"));
        cd.setDescription(config.getString(base2 + "description"));
        String val = config.getString(base2 + "max_versions");
        if (val != null && val.length() > 0) {
          cd.setMaxVersions(Integer.parseInt(val));
        }
        val = config.getString(base2 + "compression");
        if (val != null && val.length() > 0) {
          cd.setCompression(val);
        }
        val = config.getString(base2 + "in_memory");
        if (val != null && val.length() > 0) {
          cd.setInMemory(Boolean.parseBoolean(val));
        }
        val = config.getString(base2 + "block_cache_enabled");
        if (val != null && val.length() > 0) {
          cd.setBlockCacheEnabled(Boolean.parseBoolean(val));
        }
        val = config.getString(base2 + "block_size");
        if (val != null && val.length() > 0) {
          cd.setBlockSize(Integer.parseInt(val));
        }
        val = config.getString(base2 + "time_to_live");
        if (val != null && val.length() > 0) {
          cd.setTimeToLive(Integer.parseInt(val));
        }
        val = config.getString(base2 + "bloom_filter");
        if (val != null && val.length() > 0) {
          cd.setBloomFilter(val);
        }
        val = config.getString(base2 + "replication_scope");
        if (val != null && val.length() > 0) {
          //cd.setScope(Integer.parseInt(val)); Add in 0.90
          LOG.warn("Cannot set replication scope!");
        }
        ts.addColumn(cd);
      }
      schemas.add(ts);
    }
  }

  public void process() throws IOException {
    hbaseAdmin = new HBaseAdmin(conf);
    for (final TableSchema schema : schemas) {
      createOrChangeTable(schema);
    }
  }

  // cc HushSchemaManager Creating or modifying table schemas using the HBase administrative API
  // vv HushSchemaManager
  private void createOrChangeTable(final TableSchema schema)
    throws IOException {
    HTableDescriptor desc = null;
    if (tableExists(schema.getName(), false)) {
      desc = getTable(schema.getName(), false);
      LOG.info("Checking table " + desc.getNameAsString() + "...");
      final HTableDescriptor d = convertSchemaToDescriptor(schema);

      final List<HColumnDescriptor> modCols =
        new ArrayList<HColumnDescriptor>();
      for (final HColumnDescriptor cd : desc.getFamilies()) {
        final HColumnDescriptor cd2 = d.getFamily(cd.getName());
        if (cd2 != null && !cd.equals(cd2)) { // co HushSchemaManager-1-Diff Compute the differences between the XML based schema and what is currently in HBase.
          modCols.add(cd2);
        }
      }
      final List<HColumnDescriptor> delCols =
        new ArrayList<HColumnDescriptor>(desc.getFamilies());
      delCols.removeAll(d.getFamilies());
      final List<HColumnDescriptor> addCols =
        new ArrayList<HColumnDescriptor>(d.getFamilies());
      addCols.removeAll(desc.getFamilies());

      if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0 || // co HushSchemaManager-2-Check See if there are any differences in the column and table definitions.
          !hasSameProperties(desc, d)) {
        LOG.info("Disabling table...");
        hbaseAdmin.disableTable(schema.getName());
        if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0) {
          for (final HColumnDescriptor col : modCols) {
            LOG.info("Found different column -> " + col);
            hbaseAdmin.modifyColumn(schema.getName(), col.getNameAsString(), // co HushSchemaManager-3-AlterCol Alter the columns that have changed. The table was properly disabled first.
              col);
          }
          for (final HColumnDescriptor col : addCols) {
            LOG.info("Found new column -> " + col);
            hbaseAdmin.addColumn(schema.getName(), col); // co HushSchemaManager-4-AddCol Add newly defined columns.
          }
          for (final HColumnDescriptor col : delCols) {
            LOG.info("Found removed column -> " + col);
            hbaseAdmin.deleteColumn(schema.getName(), col.getNameAsString()); // co HushSchemaManager-5-DelCol Delete removed columns.
          }
        } else if (!hasSameProperties(desc, d)) {
          LOG.info("Found different table properties...");
          hbaseAdmin.modifyTable(Bytes.toBytes(schema.getName()), d); // co HushSchemaManager-6-AlterTable Alter the table itself, if there are any differences found.
        }
        LOG.info("Enabling table...");
        hbaseAdmin.enableTable(schema.getName());
        LOG.info("Table enabled");
        desc = getTable(schema.getName(), false);
        LOG.info("Table changed");
      } else {
        LOG.info("No changes detected!");
      }
    } else {
      desc = convertSchemaToDescriptor(schema);
      LOG.info("Creating table " + desc.getNameAsString() + "...");
      hbaseAdmin.createTable(desc); // co HushSchemaManager-7-CreateTable In case the table did not exist yet create it now.
      LOG.info("Table created");
    }
  }
  // ^^ HushSchemaManager

  private boolean hasSameProperties(HTableDescriptor desc1,
    HTableDescriptor desc2) {
    return desc1.isDeferredLogFlush() == desc2.isDeferredLogFlush() &&
      desc1.getMaxFileSize() == desc2.getMaxFileSize() &&
      desc1.getMemStoreFlushSize() == desc2.getMemStoreFlushSize() &&
      desc1.isReadOnly() == desc2.isReadOnly();
  }

  private HTableDescriptor convertSchemaToDescriptor(final TableSchema schema) {
    HTableDescriptor desc;
    desc = new HTableDescriptor(schema.getName());
    desc.setDeferredLogFlush(schema.isDeferredLogFlush());
    desc.setMaxFileSize(schema.getMaxFileSize());
    desc.setMemStoreFlushSize(schema.getMemStoreFlushSize());
    desc.setReadOnly(schema.isReadOnly());
    for (Map.Entry<String, String> entry : schema.getKeyValues().entrySet()) {
      desc.setValue(entry.getKey(), entry.getValue());
    }
    final Collection<ColumnDefinition> cols = schema.getColumns();
    for (final ColumnDefinition col : cols) {
      final HColumnDescriptor cd =
        new HColumnDescriptor(Bytes.toBytes(col.getColumnName()),
          col.getMaxVersions(), col.getCompression(), col.isInMemory(),
          col.isBlockCacheEnabled(), col.getBlockSize(), col.getTimeToLive(),
          col.getBloomFilter(), col.getReplicationScope());
      desc.addFamily(cd);
    }
    return desc;
  }

  private synchronized HTableDescriptor getTable(final String name,
    final boolean force) throws IOException {
    if (remoteTables == null || force) {
      remoteTables = hbaseAdmin.listTables();
    }
    for (final HTableDescriptor d : remoteTables) {
      if (d.getNameAsString().equals(name)) {
        return d;
      }
    }
    return null;
  }

  private boolean tableExists(final String name, final boolean force)
    throws IOException {
    getTables(force);
    for (final HTableDescriptor d : remoteTables) {
      if (d.getNameAsString().equals(name)) {
        return true;
      }
    }
    return false;
  }

  private void getTables(final boolean force) throws IOException {
    if (remoteTables == null || force) {
      remoteTables = hbaseAdmin.listTables();
    }
  }
}
