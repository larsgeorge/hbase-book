package pig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * <p>Transforms a Json string into a Pig map.<br>
 * Only goes 1 level deep -- all value representations are their toString()
 * representations.</p>
 *
 * From Elephant Bird: https://github.com/kevinweil/elephant-bird/blob/master/
 * src/java/com/twitter/elephantbird/pig/piggybank/JsonStringToMap.java
 */
@SuppressWarnings("rawtypes")
public class JsonStringToMap extends EvalFunc<Map> {
  private static final Logger LOG = LoggerFactory.getLogger(
    JsonStringToMap.class);
  private final JSONParser jsonParser = new JSONParser();

  @Override
  public Map<String, String> exec(Tuple input) throws IOException {
    try {
      if (input == null || input.size() < 1) {
        throw new IOException("Not enough arguments to " +
          this.getClass().getName() + ": got " + input.size() +
          ", expected at least 1");
      }

      if (input.get(0) == null) {
        return null;
      }

      String jsonLiteral = (String) input.get(0);
      return parseStringToMap(jsonLiteral);
    } catch (ExecException e) {
      LOG.warn("Error in " + getClass() + " with input " + input, e);
      throw new IOException(e);
    }
  }

  protected Map<String, String> parseStringToMap(String line) {
    try {
      Map<String, String> values = Maps.newHashMap();
      JSONObject jsonObj = (JSONObject) jsonParser.parse(line);
      for (Object key : jsonObj.keySet()) {
        Object value = jsonObj.get(key);
        values.put(key.toString(), value != null ? value.toString() : null);
      }
      return values;
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      return null;
    } catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      return null;
    }
  }

}
