package coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc DuplicateRegionObserverExample Example of attempting to load the same observer multiple times
// vv DuplicateRegionObserverExample
public class DuplicateRegionObserverExample extends BaseRegionObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  public static final byte[] FIXED_COLUMN = Bytes.toBytes("@@@GET_COUNTER@@@");
  private static AtomicInteger counter = new AtomicInteger(0);

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
    Get get, List<Cell> results) throws IOException {
    int count = counter.incrementAndGet();
    LOG.info("Current preGet count: " + count + " [" + this + "]");
  }

  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
    Get get, List<Cell> results) throws IOException {
    Put put = new Put(get.getRow());
    put.addColumn(get.getRow(), FIXED_COLUMN, Bytes.toBytes(counter.get()));
    CellScanner scanner = put.cellScanner();
    scanner.advance();
    Cell cell = scanner.current();
    LOG.debug("Adding fake cell: " + cell);
    results.add(cell);
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "master-1.internal.larsgeorge.com," +
      "master-2.internal.larsgeorge.com,master-3.internal.larsgeorge.com");
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");

    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf("testtable");

    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("colfam1"));
    htd.addCoprocessor(DuplicateRegionObserverExample.class.getCanonicalName(),
      null, Coprocessor.PRIORITY_USER, null);
    htd.addCoprocessor(DuplicateRegionObserverExample.class.getCanonicalName(),
      null, Coprocessor.PRIORITY_USER, null);

    // ^^ DuplicateRegionObserverExample
    /* Does not work as expected! Will throw the following exception:

      Exception in thread "main" java.io.IOException: Coprocessor \
        coprocessor.DuplicateRegionObserverExample already exists.
        at org.apache.hadoop.hbase.HTableDescriptor.addCoprocessor(HTableDescriptor.java:1232)
        at coprocessor.DuplicateRegionObserverExample.main(DuplicateRegionObserverExample.java:69)
        ...
        at com.intellij.rt.execution.application.AppMain.main(AppMain.java:140)
	  */
    // vv DuplicateRegionObserverExample

    Admin admin = connection.getAdmin();
    admin.createTable(htd);
    System.out.println(admin.getTableDescriptor(tableName));

    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, "colfam1");

    Table table = connection.getTable(tableName);
    Get get = new Get(Bytes.toBytes("row-1"));
    Result result = table.get(get);

    helper.dumpResult(result);

    table.close();
    admin.close();
    connection.close();
  }
}
// ^^ DuplicateRegionObserverExample

/*
Adding coprocessor using HBase shell uses its own table attribute insertion
with no check during the ALTER command:

$ hbase shell
HBase Shell; enter 'help<RETURN>' for list of supported commands.
Type "exit<RETURN>" to leave the HBase Shell
Version 1.1.1, rd0a115a7267f54e01c72c603ec53e91ec418292f, Tue Jun 23 14:44:07 PDT 2015

hbase(main):001:0> create 'testtable', 'colfam1'
0 row(s) in 2.1750 seconds

=> Hbase::Table - testtable
hbase(main):002:0> alter 'testtable', 'coprocessor' => 'file:///opt/hbase-book/hbase-book-ch04-2.0.jar|coprocessor.DuplicateRegionObserverExample|'
Updating all regions with the new schema...
0/1 regions updated.
1/1 regions updated.
Done.
0 row(s) in 3.9970 seconds

hbase(main):003:0> describe 'testtable'
Table testtable is ENABLED
testtable, {TABLE_ATTRIBUTES => {coprocessor$1 => 'file:///opt/hbase-book/hbase-book-ch04-2.0.jar|coprocessor.DuplicateRegionObserverExample|'}
COLUMN FAMILIES DESCRIPTION
{NAME => 'colfam1', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'ROW', REPLICATION_SCOPE => '0', VERSIONS => '1', COMPRESSION => 'NONE', MIN_VERSIONS => '0', TTL => 'FOREVER', KEEP_DELETED_CELLS => 'FALSE', BLOC
KSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true'}
1 row(s) in 0.0680 seconds

hbase(main):004:0> alter 'testtable', 'coprocessor' => 'file:///opt/hbase-book/hbase-book-ch04-2.0.jar|coprocessor.DuplicateRegionObserverExample|'
Updating all regions with the new schema...
1/1 regions updated.
Done.
0 row(s) in 2.1740 seconds

hbase(main):005:0> describe 'testtable'
Table testtable is ENABLED
testtable, {TABLE_ATTRIBUTES => {coprocessor$1 => 'file:///opt/hbase-book/hbase-book-ch04-2.0.jar|coprocessor.DuplicateRegionObserverExample|', coprocessor$2 => 'file:///opt/hbase-book/hbase-book-ch04-2.0.jar|copr
ocessor.DuplicateRegionObserverExample|'}
COLUMN FAMILIES DESCRIPTION
{NAME => 'colfam1', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'ROW', REPLICATION_SCOPE => '0', VERSIONS => '1', COMPRESSION => 'NONE', MIN_VERSIONS => '0', TTL => 'FOREVER', KEEP_DELETED_CELLS => 'FALSE', BLOC
KSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true'}
1 row(s) in 0.0200 seconds

hbase(main):006:0> put 'testtable', 'row-1', 'colfam1:col1', 'val1'
0 row(s) in 0.1090 seconds

hbase(main):007:0> get 'testtable', 'row-1'
COLUMN                                                 CELL
 colfam1:col1                                          timestamp=1439549519940, value=val1
 row-1:@@@GET_COUNTER@@@                               timestamp=9223372036854775807, value=\x00\x00\x00\x02
 row-1:@@@GET_COUNTER@@@                               timestamp=9223372036854775807, value=\x00\x00\x00\x02
3 row(s) in 0.0380 seconds

hbase(main):008:0> get 'testtable', 'row-1'
COLUMN                                                 CELL
 colfam1:col1                                          timestamp=1439549519940, value=val1
 row-1:@@@GET_COUNTER@@@                               timestamp=9223372036854775807, value=\x00\x00\x00\x04
 row-1:@@@GET_COUNTER@@@                               timestamp=9223372036854775807, value=\x00\x00\x00\x04
3 row(s) in 0.0200 seconds

hbase(main):009:0>

 */