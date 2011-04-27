<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.UrlManager" %>
<%@ page import="com.hbasebook.hush.table.ShortUrl" %>
<%@ page import="java.net.URL" %>
<%@ page import="java.security.Principal" %>
<%
  ShortUrl surl = null;
  String urlParam = request.getParameter("url");
  Principal principal = request.getUserPrincipal();
  
  if (urlParam != null && urlParam.length() > 0) {
    try {
      URL url = new URL (urlParam);    
      UrlManager urlm = ResourceManager.getInstance().getUrlManager();
      String username = principal == null ? null : principal.getName();
      surl = urlm.createShortUrl(url, username);      
    }
    catch (MalformedURLException e) {
      request.setAttribute("error", "Invalid URL.");
    }
  }
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">

<%@page import="java.net.MalformedURLException"%><html>
<head>
  <title>Hush!</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<jsp:include page="/include/header.jsp"/>
<div class="main">
   <h2>Welcome to the HBase URL Shortener</h2>
   <jsp:include page="/include/error.jsp"/>
   
   <div id="shorten">
     <p>Shorten your URLs!</p>

     <form action="/index.jsp" method="post">
       <textarea name="url" rows="2" cols="60"></textarea>
       <input type="submit" value="Shorten it"/>
     </form>
     <% if (surl != null) {
       String qrUrl = surl.toString() + ".q";
     %>
     <p>Your new shortened URL is:</p>

     <p><input type="text" size="50" value="<%= surl.toString() %>" disabled="disabled"/></p>

     <p>Hand it out as a QRCode:</p>

     <p><input type="text" size="50" value="<%= qrUrl %>" disabled="disabled"/></p>

     <p><img src="<%= qrUrl %>" width="100" height="100" alt=""/></p>
     <% } %>
   </div>
</div>
<!--  main -->
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
