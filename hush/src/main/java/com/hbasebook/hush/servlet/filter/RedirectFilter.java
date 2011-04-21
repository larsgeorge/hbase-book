package com.hbasebook.hush.servlet.filter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbasebook.hush.ResourceManager;
import com.hbasebook.hush.table.ShortUrlTable;

/**
 * The filter handles the short Id to URL redirects.
 */
public class RedirectFilter implements Filter {

  /**
   * Initialized the filter instance.
   *
   * @param filterConfig  The filter configuration.
   * @throws ServletException When initializing the filter fails.
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
   * @param request  The current request.
   * @param response  The response to write to.
   * @param chain  The filter chain instance.
   * @throws IOException When handling the in- or output fails.
   * @throws ServletException Might be thrown when there is an internal issue.
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
    } else {
      // otherwise assume it is a short Id - acquire resources
      ResourceManager manager = ResourceManager.getInstance();
      HTable table = manager.getTable(ShortUrlTable.NAME);
      try {
        // analyse the rest of the given URL
        String trailer = uri.substring(1);
        String[] parts = trailer.split("\\.");
        byte[] shortId = Bytes.toBytes(parts[0]);
        boolean qr = parts.length > 1 && parts[1].equalsIgnoreCase("q");
        // get the short Id to URL mapping
        Get get = new Get(shortId);
        get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL);
        get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID);
        Result result = table.get(get);
        if (!result.isEmpty()) {
          // something was found, use it to redirect
          byte[] value = result.getValue(ShortUrlTable.DATA_FAMILY,
            ShortUrlTable.URL);
          if (value.length > 0) {
            String url = Bytes.toString(value);
            // either redirect to shortened URL or send QRCode
            if (!qr) {
              // update counters
              byte[] userName = result.getValue(ShortUrlTable.DATA_FAMILY,
                ShortUrlTable.USER_ID);
              manager.getCounters().incrementUsage(userName, shortId);
              // redirect
              httpResponse.sendRedirect(url);
            } else {
              sendQRCode(httpResponse, url);
            }
          } else {
            // pathological case where there is a short Id but no URL
            httpResponse.sendError(501);
          }
        } else {
          // if the short Id was bogus show a "shush" (our fail whale)
          httpResponse.sendError(404);
        }
      } finally {
        // release resources
        manager.putTable(table);
      }
    }
  }

  /**
   * Copies a Google Chart API QRCode image to the output stream.
   *
   * @param response  The response instance to use.
   * @param url  The URL to encode.
   * @throws IOException When reading or writing the image fails.
   */
  private void sendQRCode(HttpServletResponse response, String url) throws IOException {
    URL qrUrl = new URL("http://chart.apis.google.com/chart?" +
      "chs=100x100&cht=qr&chl=" + response.encodeURL(url));
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
