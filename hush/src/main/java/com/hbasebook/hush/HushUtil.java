package com.hbasebook.hush;

import java.io.IOException;
import java.security.Principal;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HushUtil {
  /**
   * The digits used to BASE encode the short Ids.
   */
  private static final String BASE_62_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  /**
   * Converts a long to a base-62 string in reverse. (Least significant digit
   * first.)
   *
   * @param number The value to convert.
   * @return The converted value.
   */
  public static String hushEncode(long number) {
    return longToString(number, BASE_62_DIGITS, true);
  }

  /**
   * Converts a base-62 reverse string to a long.
   *
   * @param number Value to convert.
   * @return The converted value.
   */
  public static long hushDecode(String number) {
    return parseLong(number, BASE_62_DIGITS, true);
  }

  /**
   * Encodes a number in BASE N.
   *
   * @param number The number to encode.
   * @param digits The character set to use for the encoding.
   * @param reverse Flag to indicate if the result should be reversed.
   * @return The encoded - and optionally reversed - encoded string.
   */
  private static String longToString(long number, String digits,
      boolean reverse) {
    int base = digits.length();
    String result = number == 0 ? "0" : "";
    while (number != 0) {
      int mod = (int) number % base;
      if (reverse) {
        result += digits.charAt(mod);
      } else {
        result = digits.charAt(mod) + result;
      }
      number = number / base;
    }
    return result;
  }

  /**
   * Decodes the given BASE N encoded value.
   *
   * @param number The encoded value to decode.
   * @param digits The character set to decode with.
   * @param reverse Flag to indicate how the encoding was done.
   * @return The decoded number.
   */
  private static long parseLong(String number, String digits, boolean reverse) {
    int base = digits.length();
    int index = number.length();
    int result = 0;
    int multiplier = 1;
    while (index-- > 0) {
      int pos = reverse ? number.length() - (index + 1) : index;
      result += digits.indexOf(number.charAt(pos)) * multiplier;
      multiplier = multiplier * base;
    }
    return result;
  }

  /**
   * Replaces an unset string with an empty one.
   *
   * @param s The string to check.
   * @return Return the original string or an empty one.
   */
  public static String fixNull(String s) {
    if (s == null) {
      return "";
    }
    return s;
  }

  /**
   * Helps with login credentials.
   *
   * @param request The current request.
   * @param response The current response.
   * @return The user name.
   * @throws IOException When something is wrong with the request.
   */
  public static String getOrSetUsername(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    Principal principal = request.getUserPrincipal();
    String username = null;
    if (principal != null) {
      username = principal.getName();
    }
    if (username == null && request.getCookies() != null) {
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
      username = ResourceManager.getInstance().getUserManager().generateAnonymousUserId();
      response.addCookie(new Cookie("auid", username));
      // add as a request attribute so chained servlets can get to it
      request.setAttribute("auid", username);
    }
    return username;
  }

}
