<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.UrlManager" %>
<%@ page import="com.hbasebook.hush.HushUtil" %>
<%@ page import="com.hbasebook.hush.model.ShortUrl" %>
<%@ page import="java.net.URL" %>
<%@ page import="java.security.Principal" %>
<%@ page import="java.net.MalformedURLException" %>
<%@ page import="com.hbasebook.hush.servlet.RequestInfo" %>
<%
  ShortUrl surl = null;
  String urlParam = request.getParameter("url");
  if (urlParam != null && urlParam.length() > 0) {
    try {
      URL url = new URL(urlParam);
      UrlManager urlm = ResourceManager.getInstance().getUrlManager();
      String username = HushUtil.getOrSetUsername(request, response);
      surl = urlm.shorten(url, username, new RequestInfo(request));
    } catch (MalformedURLException e) {
      request.setAttribute("error", "Invalid URL.");
    }
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
<div class="wrap">
  <jsp:include page="/include/header.jsp"/>
  <div class="main">
    <h2>Welcome to the HBase URL Shortener</h2>
    <jsp:include page="/include/error.jsp"/>

    <div id="shorten">
      <p>Shorten your URLs!</p>

      <form action="/index.jsp" method="post">
        <input type="text" name="url" size="60"/>
        <input type="submit" value="Shorten it"/>
      </form>
    </div>
    <% if (surl != null) {
      String qrUrl = surl.toString() + ".q";
    %>
    <div id="short_url">
      <p>Your new shortened URL is:</p>
      <input type="text" size="50" value="<%= surl.toString() %>"
             disabled="disabled"/>
    </div>
    <% } %>
    <jsp:include page="/include/userstats.jsp"/>
  </div>
</div>
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
