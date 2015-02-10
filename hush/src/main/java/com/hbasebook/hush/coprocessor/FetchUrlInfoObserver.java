package com.hbasebook.hush.coprocessor;

import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

import java.io.IOException;

/**
 * An observer implementation that checks for new URL records and
 * asynchronously fetches some into about it.
 */
public class FetchUrlInfoObserver extends BaseRegionObserver
  implements RegionObserver, FetchUrlInfoProtocol {


  @Override
  public void processQueue() {
  }

  @Override
  public void updateUrl(String url) throws IOException {
  }

  @Override
  public void updateUrlMD5(String urlMD5) throws IOException {
  }

//  @Override
//  public long getProtocolVersion(String s, long l) throws IOException {
//    return 1L;
//  }
}
