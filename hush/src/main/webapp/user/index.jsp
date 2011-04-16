<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.client.Scan" %>
<%@ page import="com.hbasebook.hush.table.UserShortUrlTable" %>
<%@ page import="java.security.Principal" %>
<%@ page import="org.apache.hadoop.hbase.client.ResultScanner" %>
<%@ page import="org.apache.hadoop.hbase.client.Result" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="org.apache.hadoop.hbase.client.Get" %>
<%@ page import="org.apache.hadoop.hbase.client.Row" %>
<%@ page import="com.hbasebook.hush.table.ShortUrlTable" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<%
  Principal principal = request.getUserPrincipal();
  String userName = principal.getName();
  ResourceManager manager = ResourceManager.getInstance();
  HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);
  HTable shortUrltable = manager.getTable(ShortUrlTable.NAME);

  byte[] startRow = Bytes.toBytes(userName);
  byte[] one = new byte[] { 1 };
  byte[] stopRow = Bytes.add(startRow, one);

  Scan scan = new Scan(startRow, stopRow);
  scan.addFamily(UserShortUrlTable.DAILY_FAMILY);

  ResultScanner scanner = userShortUrltable.getScanner(scan);
  List<Row> gets = new ArrayList<Row>();
  for (Result result : scanner) {
    String rowKey = Bytes.toString(result.getRow());
    String shortId = rowKey.substring(rowKey.indexOf(0) + 1);
    Get get = new Get(Bytes.toBytes(shortId));
    gets.add(get);
  }
  Object[] getResults = shortUrltable.batch(gets);
  manager.putTable(userShortUrltable);
  manager.putTable(shortUrltable);
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Account Details</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<h2>My Links</h2>
<p>
<ul>
<%
  for (Object object : getResults) {
    if (object instanceof Result) {
      Result result = (Result) object;
%>
  <li>
    Short Id: <%= Bytes.toString(result.getRow()) %>
    URL: <%= Bytes.toString(result.getValue(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL)) %>
  </li>
<%
    } else {
%>
  ERROR
<%
    }
  }
%>
</ul>
</p>
</body>
</html>
