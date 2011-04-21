package coprocessor;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

// cc RowCountProtocol Example endpoint protocol, adding a row and KeyValue count method.
// vv RowCountProtocol
public interface RowCountProtocol extends CoprocessorProtocol {
  long getRowCount() throws IOException;

  long getRowCount(Filter filter) throws IOException;

  long getKeyValueCount() throws IOException;
}
// ^^ RowCountProtocol
