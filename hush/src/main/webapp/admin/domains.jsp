<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="java.util.List" %>
<%@ page import="com.hbasebook.hush.table.ShortDomain" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.DomainManager" %>
<%
  ResourceManager rm = ResourceManager.getInstance();
  DomainManager dm = new DomainManager(rm);
  
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
  <jsp:include page="/include/error.jsp"/>
  <table cellpadding="0" cellspacing="0" border="0">
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
%>
      <tr>
        <td><%= sdom.getShortDomain() %></td>
        <td>
          <table cellpadding="0" cellspacing="0" border="0">
<%
      List<String> domains = sdom.getDomains();      
	  for (String domain : domains) {
%>
          
            <tr>
              <td><%= domain %></td>
              <td><a href="/admin/domains.jsp?action=deleteLongDomain&value=<%=domain %>">[ delete ]</a></td>
            </tr>
<%
	  }
%>
          </table>
        </td>
        <td><%= sdom.getShortDomain() %></td>
        <td><a href="/admin/domains.jsp?action=deleteShortDomain&value=<%= sdom.getShortDomain() %>">[ delete ]</a></td>
      </tr>
<%
  }
%>
    </tbody>
  </table>
</div>
<jsp:include page="/include/footer.jsp"/>
</body>
</html>