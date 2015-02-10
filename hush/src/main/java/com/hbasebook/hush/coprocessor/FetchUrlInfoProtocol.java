package com.hbasebook.hush.coprocessor;

//import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * Defines a dynamic RPC call to initiate server side actions.
 */
public interface FetchUrlInfoProtocol { //extends CoprocessorProtocol {

  void processQueue();

  void updateUrl(String url) throws IOException;
  void updateUrlMD5(String urlMD5) throws IOException;
}
