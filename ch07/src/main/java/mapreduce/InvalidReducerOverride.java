package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvalidReducerOverride {

  static class InvalidOverrideReduce
  extends Reducer<Writable, Writable, Writable, Writable> {
    //@Override
    protected void reduce(Writable key, Iterator values, Context context)
      throws IOException, InterruptedException {
      context.write(key, new Text());
    }
  }

}
