package com.hbasebook.hush.servlet.filter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hbasebook.hush.ResourceManager;
import com.hbasebook.hush.table.ShortUrl;

/**
 * The filter handles the short Id to URL redirects.
 */
public class RedirectFilter implements Filter {
  public static final String QR_EXTENSION = ".q";
  public static final String DETAILS_EXTENSION = "+";

  /**
   * Initialized the filter instance.
   * 
   * @param filterConfig
   *          The filter configuration.
   * @throws ServletException
   *           When initializing the filter fails.
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // not used
  }

  /**
   * Called when the filter is decommissioned.
   */
  @Override
  public void destroy() {
    // not used
  }

  /**
   * Filter the requests. Used to detect actual pages versus shortened URL Ids.
   * 
   * @param request
   *          The current request.
   * @param response
   *          The response to write to.
   * @param chain
   *          The filter chain instance.
   * @throws IOException
   *           When handling the in- or output fails.
   * @throws ServletException
   *           Might be thrown when there is an internal issue.
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    final HttpServletResponse httpResponse = (HttpServletResponse) response;

    String uri = httpRequest.getRequestURI();
    // check if the request shall pass
    if (uri.equals("/") || uri.startsWith("/user") || uri.startsWith("/admin")
        || uri.endsWith(".jsp") || uri.endsWith(".css")) {
      chain.doFilter(request, response);
    } else if (uri.endsWith(DETAILS_EXTENSION)) {
      // redirect http://hostname/shortId+ to
      // http://hostname/user/details.jsp?sid=shortId
      String shortId = uri.substring(1, uri.length()
          - DETAILS_EXTENSION.length());
      httpResponse.sendRedirect("/user/details.jsp?sid=" + shortId);
    } else if (uri.endsWith(QR_EXTENSION)) {
      ResourceManager rm = ResourceManager.getInstance();
      String shortId = uri.substring(1, uri.length() - QR_EXTENSION.length());
      ShortUrl surl = rm.getUrlManager().getShortUrl(shortId);
      if (surl == null) {
        // if the short Id was bogus show a "shush" (our fail whale)
        httpResponse.sendError(404);
      } else {
        sendQRCode(httpResponse, surl.getLongUrl());
      }
    } else {
      // otherwise assume it is a short Id
      ResourceManager rm = ResourceManager.getInstance();
      String shortId = uri.substring(1);
      ShortUrl surl = rm.getUrlManager().getShortUrl(shortId);
      if (surl == null) {
        // if the short Id was bogus show a "shush" (our fail whale)
        httpResponse.sendError(404);
      } else {
        // increment counters and redirect to the long url!
        String user = surl.getUser();
        if (user != null) {
          rm.getCounters().incrementUsage(user, shortId);
        }
        httpResponse.sendRedirect(surl.getLongUrl());
      }
    }
  }

  /**
   * Copies a Google Chart API QRCode image to the output stream.
   * 
   * @param response
   *          The response instance to use.
   * @param url
   *          The URL to encode.
   * @throws IOException
   *           When reading or writing the image fails.
   */
  private void sendQRCode(HttpServletResponse response, String url)
      throws IOException {
    URL qrUrl = new URL("http://chart.apis.google.com/chart?"
        + "chs=100x100&cht=qr&chl=" + response.encodeURL(url));
    InputStream in = new BufferedInputStream(qrUrl.openStream());
    OutputStream out = response.getOutputStream();
    byte[] buf = new byte[1024];
    int contentLength = 0;
    while (true) {
      int length = in.read(buf);
      if (length < 0) {
        break;
      }
      out.write(buf, 0, length);
      contentLength += length;
    }
    response.setContentType("image/png");
    response.setContentLength(contentLength);
    out.flush();
  }
}
