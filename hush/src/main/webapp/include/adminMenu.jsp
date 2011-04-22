<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<div id="adminMenu">
<ul>
<%
  String uri = request.getRequestURI();
  List<String[]> menu = new ArrayList<String[]>();
  menu.add (new String [] { "domains","/admin/domains.jsp" });
  menu.add (new String [] { "users","/admin/users.jsp" });
  
  for (String [] item : menu) {
    if (uri.startsWith(item[1])) {
%>
      <li><span><%= item[0] %></span></li>
<%    
    } else {
%>
      <li><a href="<%=item[1]%>"><%= item[0] %></a></li>
<%
    }
  }
%>
</ul>
</div>
