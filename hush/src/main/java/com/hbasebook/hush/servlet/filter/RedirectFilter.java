package com.hbasebook.hush.servlet.filter;

import java.io.IOException;
import java.security.Principal;

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

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    final HttpServletResponse httpResponse = (HttpServletResponse) response;

    String uri = httpRequest.getRequestURI();
    // check if the request shall pass
    if (uri.equals("/") || uri.contains("/user") || uri.contains("/admin")
        || uri.endsWith(".jsp") || uri.endsWith(".css")) {
      chain.doFilter(request, response);
    } else {
      // otherwise assume it is a short Id - acquire resources
      ResourceManager manager = ResourceManager.getInstance();
      HTable table = manager.getTable(ShortUrlTable.NAME);
      try {
        // get the short Id to URL mapping
        byte[] shortId = Bytes.toBytes(uri.substring(1));
        Get get = new Get(shortId);
        get.addColumn(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL);
        Result result = table.get(get);
        if (!result.isEmpty()) {
          // something was found, use it to redirect
          byte[] value = result.getValue(ShortUrlTable.DATA_FAMILY,
              ShortUrlTable.URL);
          if (value.length > 0) {
            String url = Bytes.toString(value);
            httpResponse.sendRedirect(url);
          } else {
            // pathological case where there is a short Id but no URL
            httpResponse.sendError(501);
          }
          // update counters
          Principal principal = httpRequest.getUserPrincipal();
          String user = principal != null ? principal.getName() : null;
          manager.getCounters().incrementUsage(user, shortId);
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

  @Override
  public void destroy() {
  }
}
