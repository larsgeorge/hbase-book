package security;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;

import org.apache.hadoop.security.token.Token;

// cc TokenExample Shows the use of the HBase delegation token
public class TokenExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);

    // vv TokenExample
    Token<AuthenticationTokenIdentifier> token =
      TokenUtil.obtainToken(connection);
    String urlString = token.encodeToUrlString();
    System.out.println("Encoded Token: " + urlString);
    Token token2 = new Token();
    token2.decodeFromUrlString(urlString);
    // ^^ TokenExample
    connection.close();
  }
}
