<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.client.Scan" %>
<%@ page import="com.hbasebook.hush.table.UserShortUrlTable" %>
<%@ page import="java.security.Principal" %>
<%@ page import="org.apache.hadoop.hbase.client.ResultScanner" %>
<%@ page import="org.apache.hadoop.hbase.client.Result" %>
<%@ page import="com.hbasebook.hush.Counters" %>
<%@ page import="com.hbasebook.hush.HushUtil" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="com.hbasebook.hush.table.ShortUrl" %>
<%@ page import="com.hbasebook.hush.Counter" %>
<%@ page import="java.util.Date" %>
<%
  String username = HushUtil.getOrSetUsername(request, response);
  ResourceManager manager = ResourceManager.getInstance();
  HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);

  byte[] startRow = Bytes.toBytes(username);
  byte[] one = new byte[]{1};
  byte[] stopRow = Bytes.add(startRow, one);

  Scan scan = new Scan(startRow, stopRow);
  scan.addFamily(UserShortUrlTable.DAILY_FAMILY);

  ResultScanner scanner = userShortUrltable.getScanner(scan);
  List<Counters.ShortUrlStatistics> stats =
    new ArrayList<Counters.ShortUrlStatistics>();
  for (Result result : scanner) {
    String rowKey = Bytes.toString(result.getRow());
    String shortId = rowKey.substring(rowKey.indexOf(0) + 1);
    ShortUrl shortUrl = new ShortUrl(shortId, null, null, null, username);
    Counters.ShortUrlStatistics stat = manager.getCounters().getDailyStatistics(
      shortUrl, 30, 110.0);
    stats.add(stat);
  }
  manager.putTable(userShortUrltable);
  if (stats.size() > 0) {
%>
<div id="userstats">
  <p>
  <table id="tbluserstats">
    <thead>
    <tr>
      <th>No.</th>
      <th>Short Id</th>
      <th>Long URL</th>
      <th>Trend (last 30d)</th>
    </tr>
    </thead>
    <tbody>
    <%
      int rowNum = 0;
      for (Counters.ShortUrlStatistics stat : stats) {
        rowNum++;
        ShortUrl shortUrl = stat.getShortUrl();
        String shortId = shortUrl.getId();
        String detailsUrl = "/" + shortId + "+";
        StringBuffer sparkData = new StringBuffer();
        for (Object obj : stat.getCounters("clicks").descendingSet()) {
          Counter<Date, Double> counter = (Counter<Date, Double>) obj;
          if (sparkData.length() > 0) {
            sparkData.append(",");
          }
          sparkData.append(counter.getValue());
        }
    %>
    <tr>
      <td class="rowNum"><%= rowNum%></td>
      <td class="shortUrl"><a href="<%= detailsUrl %>"><%= shortId %></a></td>
      <td class="longUrl"><a href="<%= shortUrl.getLongUrl()%>" target=""><%= shortUrl.getLongUrl() %></a></td>
      <td class="trend"><a href="<%= detailsUrl %>">
        <img alt="Recent Trend for <%= shortId%>"
             src="http://chart.apis.google.com/chart?cht=ls&chs=120x15&chd=t:<%= sparkData%>&chco=999999&chm=B,999999,0,0,0&chds=0,120"/></a>
      </td>
    </tr>
    <%
      }
    %>
    </tbody>
  </table>
  </p>
</div>
<% } %>