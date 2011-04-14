<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.table.ShortUrlTable" %>
<%@ page import="org.apache.hadoop.hbase.client.Put" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="java.security.Principal" %>
<%
  String url = request.getParameter("url");
  String newShortId = null;
  Principal principal = request.getUserPrincipal();
  if (url != null && url.length() > 0) {
    ResourceManager manager = ResourceManager.getInstance();
    HTable table = manager.getTable(ShortUrlTable.NAME);
    byte[] newId = manager.getShortId();
    newShortId = Bytes.toString(newId);
    Put put = new Put(newId);
    put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL,
      Bytes.toBytes(url));
    if (principal != null) {
      put.add(ShortUrlTable.DATA_FAMILY, ShortUrlTable.USER_ID,
        Bytes.toBytes(principal.getName()));
    }
    table.put(put);
    table.flushCommits();
    manager.putTable(table);
  }
%>
<html>
<head>
  <title>Hush!</title>
  <link href="/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
<h2>Welcome to the HBase URL Shortener</h2>

<p>Shorten your URLs! Paste or type a URL into the box below and press the "Shorten" button.</p>

<p>
<form action="/index.jsp" method="post">
    URL: <input type="text" name="url"> <input type="submit" name="submit" title="Hush!">
</form>
</p>
<% if (newShortId != null) { %>
<p>Your new shortened URL is:
  <a href="http://<%=request.getHeader("Host")%>/<%= newShortId%>">http://<%=request.getHeader("Host")%>/<%= newShortId%></a>
</p>
<% } %>
<p><% if (principal != null) { %>
You are logged in as <a href="/user"><%= principal %></a>  (<a href="/logout.jsp">log out</a>).
<% } else { %>
You can track your own URLs by <a href="/signup.jsp">signing up</a> or
    <a href="/user">logging in</a>.
<% } %></p>
<p/>
<p/>
<p/>
<p>Visit <a href="http://hbasebook.com">HBase Book Online</a> for more information.</p>
</body>
</html>
