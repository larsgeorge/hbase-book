<%@ page import="java.security.Principal" %>
<%
  Principal principal = request.getUserPrincipal();
%>
<div id="header">
  <span>
  <a href="/">Home</a>&nbsp;|&nbsp;
<% if (principal != null) { %>
Welcome, <a href="/user/index.jsp"><%= principal %></a>&nbsp;|&nbsp;<a href="/logout.jsp">Sign off</a>
<% } else { %>
<a href="/signup.jsp">Sign in</a>
<% } %>
  </span>
</div>
