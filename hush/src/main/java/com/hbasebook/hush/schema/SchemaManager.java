package com.hbasebook.hush.schema;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 * This class provides support for XML based schemas. It handles creation and
 * alteration of tables based on external definition files.
 */
public class SchemaManager {
  private static final Log LOG = LogFactory.getLog(SchemaManager.class);

  // possible XML tag names, first the shared ones
  private static final String KEY_NAME = "name";
  private static final String KEY_NAMESPACE = "namespace";
  private static final String KEY_DESCRIPTION = "description";
  private static final String KEY_CONF_KEY_VALUE = "conf_key_value";
  private static final String KEY_KEY_VALUE = "key_value";
  // table related keys
  private static final String KEY_MEMSTORE_FLUSH_SIZE = "memstore_flush_size";
  private static final String KEY_SPLIT_POLICY = "split_policy";
  private static final String KEY_MAX_FILE_SIZE = "max_file_size";
  private static final String KEY_OWNER = "owner";
  private static final String KEY_READ_ONLY = "read_only";
  private static final String KEY_COMPACTION_ENABLED = "compaction_enabled";
  private static final String KEY_DURABILITY = "durability";
  private static final String KEY_REGION_REPLICATION = "region_replication";
  private static final String KEY_COPROCESSORS = "coprocessors";
  // column family related keys
  private static final String KEY_COLUMN_FAMILY = "column_family";
  private static final String KEY_MAX_VERSIONS = "max_versions";
  private static final String KEY_MIN_VERSIONS = "min_versions";
  private static final String KEY_COMPRESSION = "compression";
  private static final String KEY_COMPACTION_COMPRESSION = "compaction_compression";
  private static final String KEY_IN_MEMORY = "in_memory";
  private static final String KEY_BLOCK_CACHE_ENABLED = "block_cache_enabled";
  private static final String KEY_BLOCK_SIZE = "block_size";
  private static final String KEY_TIME_TO_LIVE = "time_to_live";
  private static final String KEY_BLOOM_FILTER = "bloom_filter";
  private static final String KEY_REPLICATION_SCOPE = "replication_scope";
  private static final String KEY_ENCODE_ON_DISK = "encode_on_disk";
  private static final String KEY_DATA_BLOCK_ENCODING = "data_block_encoding";
  private static final String KEY_CACHE_DATA_ON_WRITE = "cache_data_on_write";
  private static final String KEY_CACHE_INDEXES_ON_WRITE = "cache_indexes_on_write";
  private static final String KEY_CACHE_BLOOMS_ON_WRITE = "cache_blooms_on_write";
  private static final String KEY_EVICT_BLOCKS_ON_CLOSE = "evict_blocks_on_close";
  private static final String KEY_CACHE_DATA_IN_L1 = "cache_data_in_l1";
  private static final String KEY_PREFETCH_BLOCKS_ON_OPEN = "prefetch_blocks_on_open";
  private static final String KEY_KEEP_DELETED_CELLS = "keep_deleted_cells";
  private static final String KEY_COMPRESS_TAGS = "compress_tags";
  private static final String KEY_ENCRYPTION_TYPE = "encryption_type";
  private static final String KEY_ENCRYPTION_KEY = "encryption_key";

  private Configuration conf = null;
  private Connection connection = null;
  private Admin admin = null;
  private XMLConfiguration config = null;
  private ArrayList<NamespaceDescriptor> namespaces = null;
  private ArrayList<HTableDescriptor> schemas = null;
  private NamespaceDescriptor[] remoteNamespaces = null;
  private HTableDescriptor[] remoteTables = null;

  public SchemaManager(Configuration conf, String schemaName)
  throws ParseException, ConfigurationException, IOException {
    this.conf = conf;
    readConfiguration(schemaName);
  }

  private void readConfiguration(String schemaName)
  throws ConfigurationException, IOException {
    URL schemaUrl = Thread.currentThread().getContextClassLoader().
      getResource(schemaName);
    config = new XMLConfiguration(schemaUrl);
    namespaces = new ArrayList<NamespaceDescriptor>();
    schemas = new ArrayList<HTableDescriptor>();
    readNamespaces();
    readTableSchemas();
  }

  @SuppressWarnings("deprecation") // because of API usage, temporary
  private void readNamespaces() throws IOException {
    int maxNamespaces = config.getMaxIndex("schema.namespace");
    // parse all tables
    for (int n = 0; n <= maxNamespaces; n++) {
      // first the table descriptor
      String base = "schema.namespace(" + n + ").";
      String name = config.getString(base + KEY_NAME);
      NamespaceDescriptor.Builder ndb = NamespaceDescriptor.create(name);

      // read all generic key/value pairs into the table definition
      int idx = 0;
      while (true) {
        String kvKey = base + KEY_CONF_KEY_VALUE + (idx++ == 0 ? "" : idx);
        if (config.containsKey(kvKey)) {
          String[] kv = config.getString(kvKey).split("=", 2);
          ndb.addConfiguration(kv[0], kv.length > 1 ? kv[1] : null);
        } else {
          break;
        }
      }
      namespaces.add(ndb.build());
    }
  }

  @SuppressWarnings("deprecation") // because of API usage, temporary
  private void readTableSchemas() throws IOException {
    int maxTables = config.getMaxIndex("schema.table");
    // parse all tables
    for (int t = 0; t <= maxTables; t++) {
      // first the table descriptor
      String base = "schema.table(" + t + ").";
      TableName name = TableName.valueOf(config.getString(base + KEY_NAMESPACE),
        config.getString(base + KEY_NAME));
      HTableDescriptor htd = new HTableDescriptor(name);
      if (config.containsKey(base + KEY_DESCRIPTION)) {
        htd.setValue(KEY_DESCRIPTION, config.getString(base + "description"));
      }
      if (config.containsKey(base + KEY_OWNER)) {
        // deprecated for 0.95+ in HBASE-6188
        htd.setOwnerString(config.getString(base + KEY_OWNER));
      }
      if (config.containsKey(base + KEY_SPLIT_POLICY)) {
        htd.setRegionSplitPolicyClassName(
          config.getString(base + KEY_SPLIT_POLICY));
      }
      if (config.containsKey(base + KEY_MAX_FILE_SIZE)) {
        htd.setMaxFileSize(config.getLong(base + KEY_MAX_FILE_SIZE));
      }
      if (config.containsKey(base + KEY_MEMSTORE_FLUSH_SIZE)) {
        htd.setMemStoreFlushSize(config.getLong(base + KEY_MEMSTORE_FLUSH_SIZE));
      }
      if (config.containsKey(base + KEY_DURABILITY)) {
        htd.setDurability(
          Durability.valueOf(config.getString(base + KEY_DURABILITY)));
      }
      if (config.containsKey(base + KEY_COMPACTION_ENABLED)) {
        htd.setCompactionEnabled(
          config.getBoolean(base + KEY_COMPACTION_ENABLED));
      }
      if (config.containsKey(base + KEY_READ_ONLY)) {
        htd.setReadOnly(config.getBoolean(base + KEY_READ_ONLY));
      }
      if (config.containsKey(base + KEY_REGION_REPLICATION)) {
        htd.setRegionReplication(config.getInt(base + KEY_REGION_REPLICATION));
      }
      if (config.containsKey(base + KEY_COPROCESSORS)) {
        StringTokenizer st = new StringTokenizer(
          config.getString(base + KEY_COPROCESSORS), ",");
        while (st.hasMoreTokens()) htd.addCoprocessor(st.nextToken().trim());
      }
      // read all generic key/value pairs into the table definition
      int idx = 0;
      while (true) {
        String kvKey = base + KEY_KEY_VALUE + (idx++ == 0 ? "" : idx);
        if (config.containsKey(kvKey)) {
          String[] kv = config.getString(kvKey).split("=", 2);
          htd.setValue(kv[0], kv.length > 1 ? kv[1] : null);
        } else {
          break;
        }
      }
      // parse all column families
      int maxCols = config.getMaxIndex(base + KEY_COLUMN_FAMILY);
      for (int c = 0; c <= maxCols; c++) {
        String base2 = base + KEY_COLUMN_FAMILY + "(" + c + ").";
        HColumnDescriptor hcd = new HColumnDescriptor(config.getString(base2 + KEY_NAME));
        String val = config.getString(base2 + KEY_MAX_VERSIONS);
        if (val != null && val.length() > 0) {
          hcd.setMaxVersions(Integer.parseInt(val));
        }
        val = config.getString(base2 + KEY_COMPRESSION);
        if (val != null && val.length() > 0) {
          hcd.setCompressionType(Compression.getCompressionAlgorithmByName(val));
        }
        val = config.getString(base2 + KEY_IN_MEMORY);
        if (val != null && val.length() > 0) {
          hcd.setInMemory(Boolean.parseBoolean(val));
        }
        val = config.getString(base2 + KEY_BLOCK_CACHE_ENABLED);
        if (val != null && val.length() > 0) {
          hcd.setBlockCacheEnabled(Boolean.parseBoolean(val));
        }
        val = config.getString(base2 + KEY_BLOCK_SIZE);
        if (val != null && val.length() > 0) {
          hcd.setBlocksize(Integer.parseInt(val));
        }
        val = config.getString(base2 + KEY_TIME_TO_LIVE);
        if (val != null && val.length() > 0) {
          hcd.setTimeToLive(Integer.parseInt(val));
        }
        val = config.getString(base2 + KEY_BLOOM_FILTER);
        if (val != null && val.length() > 0) {
          hcd.setBloomFilterType(BloomType.valueOf(val));
        }
        val = config.getString(base2 + KEY_REPLICATION_SCOPE);
        if (val != null && val.length() > 0) {
          //hcd.setScope(Integer.parseInt(val)); Add in 0.90
          LOG.warn("Cannot set replication scope!");
        }
        // read all generic key/value pairs into the family definition
        idx = 0;
        while (true) {
          String kvKey = base2 + KEY_KEY_VALUE + (idx++ == 0 ? "" : idx);
          if (config.containsKey(kvKey)) {
            String[] kv = config.getString(kvKey).split("=", 2);
            hcd.setValue(kv[0], kv.length > 1 ? kv[1] : null);
          } else {
            break;
          }
        }
        htd.addFamily(hcd);
      }
      schemas.add(htd);
    }
  }

  /**
   * The main processing starts here.
   *
   * @throws IOException When creating a connection to HBase fails.
   */
  public void process() throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    admin = connection.getAdmin();
    for (final NamespaceDescriptor descriptor : namespaces) {
      createOrChangeNamespace(descriptor);
    }
    for (final HTableDescriptor schema : schemas) {
      createOrChangeTable(schema);
    }
  }

  private void createOrChangeNamespace(final NamespaceDescriptor descriptor)
  throws IOException {
    NamespaceDescriptor desc = null;
    if (namespaceExists(descriptor.getName(), false)) {
      desc = getNamespace(descriptor.getName(), false);
      LOG.info("Checking namespace " + desc.getName() + "...");
      if (!desc.equals(descriptor)) {
        admin.modifyNamespace(descriptor);
        LOG.info("Namespace changed");
      } else {
        LOG.info("No changes detected!");
      }
    } else {
      LOG.info("Creating namespace " + descriptor.getName() + "...");
      admin.createNamespace(descriptor);
      LOG.info("Namespace created");
    }
  }

  // cc HushSchemaManager Creating or modifying table schemas using the HBase administrative API
  // vv HushSchemaManager
  private void createOrChangeTable(final HTableDescriptor schema)
  throws IOException {
    HTableDescriptor desc = null;
    if (tableExists(schema.getTableName(), false)) {
      desc = getTable(schema.getTableName(), false);
      LOG.info("Checking table " + desc.getNameAsString() + "...");

      final List<HColumnDescriptor> modCols =
        new ArrayList<HColumnDescriptor>();
      for (final HColumnDescriptor cd : desc.getFamilies()) {
        final HColumnDescriptor cd2 = schema.getFamily(cd.getName());
        if (cd2 != null && !cd.equals(cd2)) { // co HushSchemaManager-1-Diff Compute the differences between the XML based schema and what is currently in HBase.
          modCols.add(cd2);
        }
      }
      final List<HColumnDescriptor> delCols =
        new ArrayList<HColumnDescriptor>(desc.getFamilies());
      delCols.removeAll(schema.getFamilies());
      final List<HColumnDescriptor> addCols =
        new ArrayList<HColumnDescriptor>(schema.getFamilies());
      addCols.removeAll(desc.getFamilies());

      if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0 || // co HushSchemaManager-2-Check See if there are any differences in the column and table definitions.
          !hasSameProperties(desc, schema)) {
        LOG.info("Disabling table...");
        admin.disableTable(schema.getTableName());
        if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0) {
          for (final HColumnDescriptor col : modCols) {
            LOG.info("Found different column -> " + col);
            admin.modifyColumn(schema.getTableName(), col); // co HushSchemaManager-3-AlterCol Alter the columns that have changed. The table was properly disabled first.
          }
          for (final HColumnDescriptor col : addCols) {
            LOG.info("Found new column -> " + col);
            admin.addColumn(schema.getTableName(), col); // co HushSchemaManager-4-AddCol Add newly defined columns.
          }
          for (final HColumnDescriptor col : delCols) {
            LOG.info("Found removed column -> " + col);
            admin.deleteColumn(schema.getTableName(), col.getName()); // co HushSchemaManager-5-DelCol Delete removed columns.
          }
        } else if (!hasSameProperties(desc, schema)) {
          LOG.info("Found different table properties...");
          admin.modifyTable(schema.getTableName(), schema); // co HushSchemaManager-6-AlterTable Alter the table itself, if there are any differences found.
        }
        LOG.info("Enabling table...");
        admin.enableTable(schema.getTableName());
        LOG.info("Table enabled");
        getTable(schema.getTableName(), false);
        LOG.info("Table changed");
      } else {
        LOG.info("No changes detected!");
      }
    } else {
      LOG.info("Creating table " + schema.getNameAsString() + "...");
      admin.createTable(schema); // co HushSchemaManager-7-CreateTable In case the table did not exist yet create it now.
      LOG.info("Table created");
    }
  }
  // ^^ HushSchemaManager

  private boolean hasSameProperties(HTableDescriptor desc1,
    HTableDescriptor desc2) {
    return //desc1.isDeferredLogFlush() == desc2.isDeferredLogFlush() &&
      desc1.getMaxFileSize() == desc2.getMaxFileSize() &&
      desc1.getMemStoreFlushSize() == desc2.getMemStoreFlushSize() &&
      desc1.isReadOnly() == desc2.isReadOnly();
  }

  private synchronized NamespaceDescriptor getNamespace(String name,
    final boolean force) throws IOException {
    getNamespaces(force);
    for (NamespaceDescriptor d : remoteNamespaces) {
      if (d.getName().equals(name)) {
        return d;
      }
    }
    return null;
  }

  private boolean namespaceExists(String name, final boolean force)
  throws IOException {
    return getNamespace(name, force) != null ? true : false;
  }

  private void getNamespaces(final boolean force) throws IOException {
    if (remoteNamespaces == null || force) {
      remoteNamespaces = admin.listNamespaceDescriptors();
    }
  }

  private synchronized HTableDescriptor getTable(final TableName name,
    final boolean force) throws IOException {
    getTables(force);
    for (final HTableDescriptor d : remoteTables) {
      if (d.getTableName().equals(name)) {
        return d;
      }
    }
    return null;
  }

  private boolean tableExists(final TableName name, final boolean force)
  throws IOException {
    return getTable(name, force) != null ? true : false;
  }

  private void getTables(final boolean force) throws IOException {
    if (remoteTables == null || force) {
      remoteTables = admin.listTables();
    }
  }
}
