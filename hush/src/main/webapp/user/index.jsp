<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.client.Scan" %>
<%@ page import="com.hbasebook.hush.table.UserShortUrlTable" %>
<%@ page import="java.security.Principal" %>
<%@ page import="org.apache.hadoop.hbase.client.ResultScanner" %>
<%@ page import="org.apache.hadoop.hbase.client.Result" %>
<%@ page import="com.hbasebook.hush.Counters" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<%
  Principal principal = request.getUserPrincipal();
  String userName = principal.getName();
  ResourceManager manager = ResourceManager.getInstance();
  HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);

  byte[] startRow = Bytes.toBytes(userName);
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
    Counters.ShortUrlStatistics stat = manager.getCounters().getDailyStatistics(
      Bytes.toBytes(userName), Bytes.toBytes(shortId), 30, 110.0);
    stats.add(stat);
  }
  manager.putTable(userShortUrltable);
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Account Details</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<jsp:include page="/include/header.jsp"/>
<div class="main">
  <h2>My Links</h2>

  <p>
  <table style="width: 80%">
    <thead>
    <tr  style="text-align: left">
      <th>No.</th>
      <th>Short Id</th>
      <th>Long URL</th>
      <th>Trend (Last 30 Days)</th>
    </tr>
    </thead>
    <tbody>
    <%
      int rowNum = 0;
      for (Counters.ShortUrlStatistics stat : stats) {
        rowNum++;
        String shortId = stat.getShortId();
        String detailsUrl = "/user/details.jsp?sid=" + shortId;
        StringBuffer sparkData = new StringBuffer();
        for (Double clicks : stat.getClicks().descendingMap().values()) {
          if (sparkData.length() > 0) {
            sparkData.append(",");
          }
          sparkData.append(clicks);
        }
    %>
    <tr>
      <td><%= rowNum%>
      </td>
      <td><a href="<%= detailsUrl %>"><%= shortId %>
      </a></td>
      <td><a href="<%= stat.getUrl()%>" target=""><%= stat.getUrl()%>
      </a></td>
      <td><a href="<%= detailsUrl %>">
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
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
