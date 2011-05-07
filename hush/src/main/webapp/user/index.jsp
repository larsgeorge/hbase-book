<%@ page contentType="text/html;charset=UTF-8"%>
<%@ page import="com.hbasebook.hush.HushUtil"%>
<%@ page import="com.hbasebook.hush.model.User"%>
<%@ page import="com.hbasebook.hush.ResourceManager"%>
<%@ page import="com.hbasebook.hush.UserManager"%>
<%
  String action = HushUtil.fixNull(request.getParameter("action"));
  String error = null;
  User user = null;
  String postUri = "/admin/users.jsp";
  boolean isAdmin = true;

  UserManager um = ResourceManager.getInstance().getUserManager();
  String usernameParameter = HushUtil.fixNull(request
      .getParameter("username"));
  User principalUser = um.getUser(request.getUserPrincipal().getName());
  if (usernameParameter.length() > 0 && principalUser.isAdmin()) {
    user = um.getUser(usernameParameter);
  }
  if (user == null) {
    user = principalUser;
    postUri = "/user";
    isAdmin = false;
  }

  String username = HushUtil.fixNull(user.getUsername());
  String firstName = HushUtil.fixNull(user.getFirstName());
  String lastName = HushUtil.fixNull(user.getLastName());
  String email = HushUtil.fixNull(user.getEmail());

  if (action.equalsIgnoreCase("edit")) {
    firstName = HushUtil.fixNull(request.getParameter("firstName"));
    lastName = HushUtil.fixNull(request.getParameter("lastName"));
    email = HushUtil.fixNull(request.getParameter("email"));

    if (firstName.length() == 0) {
      error = "First name is required.";
    }
    if (lastName.length() == 0) {
      error = "Last name is required.";
    }
    if (email.length() == 0) {
      error = "Email is required.";
    }
    if (error == null) {
      um.updateUser(username, firstName, lastName, email);
      response.sendRedirect(postUri);
      return;
    }
  } else if (action.equalsIgnoreCase("changePassword")) {
    String currentPassword = HushUtil.fixNull(request
        .getParameter("currentPassword"));
    String password = HushUtil.fixNull(request.getParameter("password"));
    String confirmPassword = HushUtil.fixNull(request
        .getParameter("confirmPassword"));

    if (currentPassword.length() == 0 && !isAdmin) {
      error = "Password is required";
    }
    if (password.length() == 0 || !password.equals(confirmPassword)) {
      error = "New and confirm passwords do not match";
    }
    if (error == null) {
      if (isAdmin) {
        um.adminChangePassword(username, password);
        response.sendRedirect(postUri);
        return;
      }
      // else
      if (um.changePassword(username, currentPassword, password)) {
        um.adminChangePassword(username, password);
        response.sendRedirect(postUri);
        return;
      }
      // else
      error = "Password is invalid";
    }
  }

  if (error != null) {
    request.setAttribute("error", error);
  }
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Sign up</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<div class="wrap">
<jsp:include page="/include/header.jsp"/>
<div class="main">
  <jsp:include page="/include/error.jsp"/>
  <div id="stylized" class="myform">
    <form id="form" method="post" name="form" action="/user/index.jsp">
      <h1>Profile information</h1>
      <p></p>

      <label>Username</label>
      <input type="text" name="displayUsername" id="username"
             value="<%=username%>" disabled="disabled"/>

      <label>First Name</label>
      <input type="text" name="firstName" id="firstName"
             value="<%=firstName%>"/>

      <label>Last Name</label>
      <input type="text" name="lastName" id="lastName"
             value="<%=lastName%>"/>

      <label>Email</label>
      <input type="text" name="email" id="email"
             value="<%=email%>"/>

      <input type="hidden" name="username" value="<%= username %>"/>
      <input type="hidden" name="action" value="edit"/>
      <button type="submit">Update profile</button>
      <div class="spacer"></div>
    </form>
    <form id="form" method="post" name="form" action="/user/index.jsp">
      <h1>Change Password</h1>
      <p></p>

<%
  if (!isAdmin) {
%>
      <label>Current Password</label>
      <input type="password" name="currentPassword" id="currentPassword"/>
<%
  }
%>
      <label>New Password
        <span class="small">Make it good!</span>
      </label>
      <input type="password" name="password" id="password"/>

      <label>Confirm Password</label>
      <input type="password" name="confirmPassword" id="confirmPassword"/>

      <input type="hidden" name="username" id="username"
             value="<%=username%>"/>
      <input type="hidden" name="action" value="changePassword"/>
      <button name="submit" type="submit">Change password</button>
      <div class="spacer"></div>
    </form>
  </div>
</div>
</div>
</body>
</html>