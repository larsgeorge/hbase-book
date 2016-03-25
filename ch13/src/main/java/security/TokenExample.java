package security;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;

import org.apache.hadoop.security.UserGroupInformation;
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
    File temp = new File(FileUtils.getTempDirectory(), "token");
    FileUtils.writeStringToFile(temp, urlString);

    System.out.println("Encoded Token: " + urlString);

    String strToken = FileUtils.readFileToString(new File("token"));
    Token token2 = new Token();
    token2.decodeFromUrlString(strToken);
    UserGroupInformation.getCurrentUser().addToken(token2);
    // ^^ TokenExample
    connection.close();
  }
}

/*

Exception in thread "main" org.apache.hadoop.hbase.exceptions.UnknownProtocolException: org.apache.hadoop.hbase.exceptions.UnknownProtocolException: No registered coprocessor service found for name AuthenticationService in region hbase:meta,,1
	at org.apache.hadoop.hbase.regionserver.HRegion.execService(HRegion.java:7739)
	at org.apache.hadoop.hbase.regionserver.RSRpcServices.execServiceOnRegion(RSRpcServices.java:1988)
	at org.apache.hadoop.hbase.regionserver.RSRpcServices.execService(RSRpcServices.java:1970)
	at org.apache.hadoop.hbase.protobuf.generated.ClientProtos$ClientService$2.callBlockingMethod(ClientProtos.java:33652)
	at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:2170)
	at org.apache.hadoop.hbase.ipc.CallRunner.run(CallRunner.java:109)
	at org.apache.hadoop.hbase.ipc.RpcExecutor.consumerLoop(RpcExecutor.java:133)
	at org.apache.hadoop.hbase.ipc.RpcExecutor$1.run(RpcExecutor.java:108)
	at java.lang.Thread.run(Thread.java:745)

*/