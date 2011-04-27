package coprocessor;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// cc RowCountEndpoint Example endpoint implementation, adding a row and KeyValue count method.
// vv RowCountEndpoint
public class RowCountEndpoint extends BaseEndpointCoprocessor
  implements RowCountProtocol {

  private long getCount(Filter filter, boolean countKeyValues)
    throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    if (filter != null) {
      scan.setFilter(filter);
    }
    RegionCoprocessorEnvironment environment =
      (RegionCoprocessorEnvironment) getEnvironment();
    // use an internal scanner to perform scanning.
    InternalScanner scanner = environment.getRegion().getScanner(scan);
    int result = 0;
    try {
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean done = false;
      do {
        curVals.clear();
        done = scanner.next(curVals);
        result += countKeyValues ? curVals.size() : 1;
      } while (done);
    } finally {
      scanner.close();
    }
    return result;
  }

  @Override
  public long getRowCount() throws IOException {
    return getRowCount(new FirstKeyOnlyFilter());
  }

  @Override
  public long getRowCount(Filter filter) throws IOException {
    return getCount(filter, false);
  }

  @Override
  public long getKeyValueCount() throws IOException {
    return getCount(null, true);
  }
}
// ^^ RowCountEndpoint
