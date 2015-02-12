package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Used by the book examples to generate tables and fill them with test data.
 */
public class HBaseHelper {

  private Configuration conf = null;
  private Connection connection = null;
  private Admin admin = null;

  protected HBaseHelper(Configuration conf) throws IOException {
    this.conf = conf;
    this.connection = ConnectionFactory.createConnection(conf);
    this.admin = connection.getAdmin();
  }

  public static HBaseHelper getHelper(Configuration conf) throws IOException {
    return new HBaseHelper(conf);
  }

  public void close() throws IOException {
    connection.close();
  }

  public Connection getConnection() {
    return connection;
  }

  public boolean existsTable(String table)
  throws IOException {
    return existsTable(TableName.valueOf(table));
  }

  public boolean existsTable(TableName table)
  throws IOException {
    return admin.tableExists(table);
  }

  public void createTable(String table, String... colfams)
  throws IOException {
    createTable(TableName.valueOf(table), null, colfams);
  }

  public void createTable(TableName table, String... colfams)
  throws IOException {
    createTable(table, null, colfams);
  }

  public void createTable(String table, byte[][] splitKeys, String... colfams)
  throws IOException {
    createTable(TableName.valueOf(table), splitKeys, colfams);
  }

  public void createTable(TableName table, byte[][] splitKeys, String... colfams)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(table);
    for (String cf : colfams) {
      HColumnDescriptor coldef = new HColumnDescriptor(cf);
      desc.addFamily(coldef);
    }
    if (splitKeys != null) {
      admin.createTable(desc, splitKeys);
    } else {
      admin.createTable(desc);
    }
  }

  public void disableTable(String table) throws IOException {
    disableTable(TableName.valueOf(table));
  }

  public void disableTable(TableName table) throws IOException {
    admin.disableTable(table);
  }

  public void dropTable(String table) throws IOException {
    dropTable(TableName.valueOf(table));
  }

  public void dropTable(TableName table) throws IOException {
    if (existsTable(table)) {
      disableTable(table);
      admin.deleteTable(table);
    }
  }

  public void fillTable(String table, int startRow, int endRow, int numCols,
                        String... colfams)
  throws IOException {
    fillTable(TableName.valueOf(table), startRow,endRow, numCols, colfams);
  }

  public void fillTable(TableName table, int startRow, int endRow, int numCols,
                        String... colfams)
  throws IOException {
    fillTable(table, startRow, endRow, numCols, -1, false, colfams);
  }

  public void fillTable(String table, int startRow, int endRow, int numCols,
                        boolean setTimestamp, String... colfams)
  throws IOException {
    fillTable(TableName.valueOf(table), startRow, endRow, numCols, -1,
      setTimestamp, colfams);
  }

  public void fillTable(TableName table, int startRow, int endRow, int numCols,
                        boolean setTimestamp, String... colfams)
  throws IOException {
    fillTable(table, startRow, endRow, numCols, -1, setTimestamp, colfams);
  }

  public void fillTable(String table, int startRow, int endRow, int numCols,
                        int pad, boolean setTimestamp, String... colfams)
  throws IOException {
    fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
      setTimestamp, false, colfams);
  }

  public void fillTable(TableName table, int startRow, int endRow, int numCols,
                        int pad, boolean setTimestamp, String... colfams)
  throws IOException {
    fillTable(table, startRow, endRow, numCols, pad, setTimestamp, false,
      colfams);
  }

  public void fillTable(String table, int startRow, int endRow, int numCols,
                        int pad, boolean setTimestamp, boolean random,
                        String... colfams)
  throws IOException {
    fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
      setTimestamp, random, colfams);
  }

  public void fillTable(TableName table, int startRow, int endRow, int numCols,
                        int pad, boolean setTimestamp, boolean random,
                        String... colfams)
  throws IOException {
    Table tbl = connection.getTable(table);
    Random rnd = new Random();
    for (int row = startRow; row <= endRow; row++) {
      for (int col = 0; col < numCols; col++) {
        Put put = new Put(Bytes.toBytes("row-" + padNum(row, pad)));
        for (String cf : colfams) {
          String colName = "col-" + padNum(col, pad);
          String val = "val-" + (random ?
            Integer.toString(rnd.nextInt(numCols)) :
            padNum(row, pad) + "." + padNum(col, pad));
          if (setTimestamp) {
            put.add(Bytes.toBytes(cf), Bytes.toBytes(colName),
              col, Bytes.toBytes(val));
          } else {
            put.add(Bytes.toBytes(cf), Bytes.toBytes(colName),
              Bytes.toBytes(val));
          }
        }
        tbl.put(put);
      }
    }
    tbl.close();
  }

  public String padNum(int num, int pad) {
    String res = Integer.toString(num);
    if (pad > 0) {
      while (res.length() < pad) {
        res = "0" + res;
      }
    }
    return res;
  }

  public void put(String table, String row, String fam, String qual,
                  String val) throws IOException {
    put(TableName.valueOf(table), row, fam, qual, val);
  }

  public void put(TableName table, String row, String fam, String qual,
                  String val) throws IOException {
    Table tbl = connection.getTable(table);
    Put put = new Put(Bytes.toBytes(row));
    put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
    tbl.put(put);
    tbl.close();
  }

  public void put(String table, String row, String fam, String qual, long ts,
                  String val) throws IOException {
    put(TableName.valueOf(table), row, fam, qual, ts, val);
  }

  public void put(TableName table, String row, String fam, String qual, long ts,
                  String val) throws IOException {
    Table tbl = connection.getTable(table);
    Put put = new Put(Bytes.toBytes(row));
    put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), ts,
            Bytes.toBytes(val));
    tbl.put(put);
    tbl.close();
  }

  public void put(String table, String[] rows, String[] fams, String[] quals,
                  long[] ts, String[] vals) throws IOException {
    put(TableName.valueOf(table), rows, fams, quals, ts, vals);
  }

  public void put(TableName table, String[] rows, String[] fams, String[] quals,
                  long[] ts, String[] vals) throws IOException {
    Table tbl = connection.getTable(table);
    for (String row : rows) {
      Put put = new Put(Bytes.toBytes(row));
      for (String fam : fams) {
        int v = 0;
        for (String qual : quals) {
          String val = vals[v < vals.length ? v : vals.length - 1];
          long t = ts[v < ts.length ? v : ts.length - 1];
          System.out.println("Adding: " + fam + " " + qual + " " + t + " " + val);
          put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
            Bytes.toBytes(val));
          v++;
        }
      }
      tbl.put(put);
    }
    tbl.close();
  }

  public void dump(String table, String[] rows, String[] fams, String[] quals)
  throws IOException {
    dump(TableName.valueOf(table), rows, fams, quals);
  }

  public void dump(TableName table, String[] rows, String[] fams, String[] quals)
  throws IOException {
    Table tbl = connection.getTable(table);
    List<Get> gets = new ArrayList<Get>();
    for (String row : rows) {
      Get get = new Get(Bytes.toBytes(row));
      get.setMaxVersions();
      if (fams != null) {
        for (String fam : fams) {
          for (String qual : quals) {
            get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
          }
        }
      }
      gets.add(get);
    }
    Result[] results = tbl.get(gets);
    for (Result result : results) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell +
          ", Value: " + Bytes.toString(cell.getValueArray(),
          cell.getValueOffset(), cell.getValueLength()));
      }
    }
    tbl.close();
  }
}
