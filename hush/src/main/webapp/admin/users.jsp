<%@ page contentType="text/html;charset=UTF-8"%>
<%@ page import="java.util.List"%>
<%@ page import="com.hbasebook.hush.model.User"%>
<%@ page import="com.hbasebook.hush.HushUtil"%>
<%@ page import="com.hbasebook.hush.ResourceManager"%>
<%@ page import="com.hbasebook.hush.UserManager"%>
<%
  UserManager um = ResourceManager.getInstance().getUserManager();
  List<User> list = um.getUsers();
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
<title>Users</title>
<link href="/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
<div class="wrap">
<jsp:include page="/include/header.jsp" />
<div class="main">
<jsp:include page="/include/adminMenu.jsp" />
<jsp:include page="/include/error.jsp" />
<table id="users">
	<thead>
		<tr>
			<th>Username</th>
			<th>Name</th>
			<th>E-mail</th>
			<th>Roles</th>
		</tr>
	</thead>
	<tbody>
		<%
		  for (User user : list) {
		    String fullName = HushUtil.fixNull(user.getFirstName()) + ' '
		        + HushUtil.fixNull(user.getLastName());
		%>
		<tr>
			<td class="username"><a href="/user/index.jsp?username=<%= user.getUsername() %>">
			  <%= user.getUsername() %></a></td>
			<td class="name"><%= fullName %></td>
			<td class="email"><%=HushUtil.fixNull(user.getEmail())%></td>
			<td class="roles"><%=HushUtil.fixNull(user.getRoles())%></td>
		</tr>
		<%
		  }
		%>
	</tbody>
</table>
</div>
</div>
<jsp:include page="/include/footer.jsp" />
</body>
</html>