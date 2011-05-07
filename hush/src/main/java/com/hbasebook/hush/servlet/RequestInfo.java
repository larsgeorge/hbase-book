package com.hbasebook.hush.servlet;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores the request details needed for statistics.
 */
public class RequestInfo {

  public enum InfoName { RemoteAddr, UserAgent }

  private final Map<InfoName, String> info;

  public RequestInfo() {
    info = new HashMap<InfoName, String>();
  }

  public RequestInfo(Map<InfoName, String> info) {
    this.info = info;
  }

  public RequestInfo(HttpServletRequest request) {
    info = new HashMap<InfoName, String>();
    if (request != null) {
      info.put(InfoName.RemoteAddr, request.getRemoteAddr());
    }
  }

  public String get(InfoName name) {
    return info.get(name);
  }
}
