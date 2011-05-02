package com.hbasebook.hush.servlet;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores the request details needed for statistics.
 */
public class RequestInfo {

  public enum Name { RemoteAddr, UserAgent }

  private Map<Name, String> info = new HashMap<Name, String>();

  public RequestInfo(HttpServletRequest request) {
    info.put(Name.RemoteAddr, request.getRemoteAddr());
  }

  public String get(Name name) {
    return info.get(name);
  }
}
