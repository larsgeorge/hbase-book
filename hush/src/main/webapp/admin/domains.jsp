<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="java.util.List" %>
<%@ page import="com.hbasebook.hush.table.ShortDomain" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.DomainManager" %>
<%
  DomainManager dm = ResourceManager.getInstance().getDomainManager();  
  List<ShortDomain> list = dm.listShortDomains();
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Domains</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<jsp:include page="/include/header.jsp"/>
<div class="main">
  <div align="center">
  <h2>Domains</h2>
  <jsp:include page="/include/error.jsp"/>
  
  <form action="/admin/domains.jsp">  
  <table id="domains">
    <thead>
      <tr>
        <td>Short Domain</td>
        <td>Original Domain</td>
        <td>&nbsp;</td>
      </tr>
    </thead>
    <tbody>
<%
  for (ShortDomain sdom : list) {
      List<String> ldoms = sdom.getDomains();      
%>
      <tr>
        <td class="shortDomain"><%= sdom.getShortDomain() %></td>
        <td class="longDomain">
          <ul>
<%
	  for (String ldom : ldoms) {
%>
			<li><%= ldom %></li>          
<%
	  }
%>
          </ul>
        </td>
        <td class="action">
          <ul>
<%
	  for (String ldom : ldoms) {
%>
            <li></li>[ <a href="/admin/domains.jsp?action=delete&ldom=<%= ldom %>">delete</a> ]</li>
<%
	  }
%>
          </ul>
        </td>
      </tr>
<%
  }
%>
      <tr>
        <td><input type="text" size="20" name="sdom"/></td>
        <td><input type="text" size="40" name="ldom"/></td>
        <td><input type="submit" value="Add"/></td>
      </tr>
    </tbody>
  </table>
  </form>
  
  </div> 
</div>
<jsp:include page="/include/footer.jsp"/>
</body>
</html>