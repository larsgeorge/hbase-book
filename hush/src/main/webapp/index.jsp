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
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Hush!</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<jsp:include page="/include/header.jsp"/>
<div class="main">

  <div align="center">
    <h2>Welcome to the HBase URL Shortener</h2>

    <div id="shorten">
      <p>Shorten your URLs!</p>

      <form action="/index.jsp" method="post">
        <input type="text" size="80" name="url"/>
        <input type="submit" value="Shorten it"/>
      </form>
      <% if (newShortId != null) {
        String newUrl = "http://" + request.getHeader("Host") + "/" + newShortId;
        String qrNewUrl = newUrl + ".q";
      %>
      <p>Your new shortened URL is:</p>

      <p><input type="text" size="50" value="<%= newUrl %>" disabled="disabled"/></p>

      <p>Hand it out as a QRCode:</p>

      <p><input type="text" size="50" value="<%= qrNewUrl %>" disabled="disabled"/></p>

      <p><img src="<%= qrNewUrl%>" width="100" height="100" alt=""/></p>
      <% } %>
    </div>
  </div>

</div>
<!--  main -->
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
