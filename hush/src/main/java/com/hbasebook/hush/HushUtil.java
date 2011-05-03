package com.hbasebook.hush;

import java.io.IOException;
import java.security.Principal;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HushUtil {
  public static String fixNull(String s) {
    if (s == null) {
      return "";
    }
    return s;
  }

  public static String getOrSetUsername(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    Principal principal = request.getUserPrincipal();
    String username = null;
    if (principal != null) {
      username = principal.getName();
    }
    if (username == null) {
      // no principal found
      for (Cookie cookie : request.getCookies()) {
        if (cookie.getName().equals("auid")) {
          username = cookie.getValue();
        }
      }
    }
    if (username == null) {
      // no principal and no cookie found in request
      // check response first, maybe an enclosing jsp set it
      username = (String) request.getAttribute("auid");
    }
    if (username == null) {
      // we really don't have one,
      // let's create a new cookie
      username = ResourceManager.getInstance().getCounters()
          .getAnonymousUserId()
          + ":anon";
      response.addCookie(new Cookie("auid", username));
      // add as a request attribute so chained servlets can get to it
      request.setAttribute("auid", username);
    }
    return username;
  }

}
