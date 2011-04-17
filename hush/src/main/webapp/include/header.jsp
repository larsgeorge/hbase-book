<%@ page import="java.security.Principal" %>
<%
  Principal principal = request.getUserPrincipal();
%>
<div id="header">
  <a href="/">Home</a> -
<% if (principal != null) { %>
  <span>
    Welcome, <a href="/user"><%= principal %></a>!&nbsp;&nbsp;&nbsp;<a href="/logout.jsp">Sign off</a>
  </span>
<% } else { %>
  <span>
    <a href="/signup.jsp">Sign in</a>
  </span>
<% } %>
</div>
