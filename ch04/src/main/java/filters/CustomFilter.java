package filters;

// cc CustomFilter Implements a filter that lets certain rows pass
import com.google.protobuf.InvalidProtocolBufferException;

import filters.generated.FilterProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

/**
 * Implements a custom filter for HBase. It takes a value and compares
 * it with every value in each KeyValue checked. Once there is a match
 * the entire row is passed, otherwise filtered out.
 */
// vv CustomFilter
public class CustomFilter extends FilterBase {

  private byte[] value = null;
  private boolean filterRow = true;

  public CustomFilter() {
    super();
  }

  public CustomFilter(byte[] value) {
    this.value = value; // co CustomFilter-1-SetValue Set the value to compare against.
  }

  @Override
  public void reset() {
    this.filterRow = true; // co CustomFilter-2-Reset Reset filter flag for each new row being tested.
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    if (CellUtil.matchingValue(cell, value)) {
      filterRow = false; // co CustomFilter-3-Filter When there is a matching value, then let the row pass.
    }
    return ReturnCode.INCLUDE; // co CustomFilter-4-Include Always include, since the final decision is made later.
  }

  @Override
  public boolean filterRow() {
    return filterRow; // co CustomFilter-5-FilterRow Here the actual decision is taking place, based on the flag status.
  }

  @Override
  public byte [] toByteArray() {
    FilterProtos.CustomFilter.Builder builder =
      FilterProtos.CustomFilter.newBuilder();
    if (value != null) builder.setValue(ByteStringer.wrap(value)); // co CustomFilter-6-Write Writes the given value out so it can be sent to the servers.
    return builder.build().toByteArray();
  }

  //@Override
  public static Filter parseFrom(final byte[] pbBytes)
  throws DeserializationException {
    FilterProtos.CustomFilter proto;
    try {
      proto = FilterProtos.CustomFilter.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new CustomFilter(proto.getValue().toByteArray());
  }
}
// ^^ CustomFilter
