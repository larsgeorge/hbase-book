<%
  String error = request.getParameter("error");
  if (error != null) {
%>
	<div id="error">
	  <p><%= error %></p>
	</div>
<%
  }
%>
