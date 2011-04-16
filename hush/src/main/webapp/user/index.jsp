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
<%@ page contentType="text/html;charset=UTF-8" %>
<%
  Principal principal = request.getUserPrincipal();
  String userName = principal.getName();
  ResourceManager manager = ResourceManager.getInstance();
  HTable table = manager.getTable(UserShortUrlTable.NAME);

  byte[] startRow = Bytes.toBytes(userName);
  byte[] one = new byte[] { 1 };
  byte[] stopRow = Bytes.add(startRow, one);

  Scan scan = new Scan(startRow, stopRow);
  scan.addFamily(UserShortUrlTable.DAILY_FAMILY);

  ResultScanner scanner = table.getScanner(scan);
  List<String> keys = new ArrayList<String>();
  for (Result result : scanner) {
    String rowKey = Bytes.toString(result.getRow());
    keys.add(rowKey.substring(rowKey.indexOf(0) + 1));
  }
  manager.putTable(table);
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
  for (String key : keys) {
%>
  <li>Short Id: <%= key %></li>
<% } %>
</ul>
</p>
</body>
</html>
