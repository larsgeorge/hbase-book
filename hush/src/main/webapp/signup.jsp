<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.HushUtil" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.UserManager" %>
<%@ page import="java.util.BitSet" %>
<%
  String action = HushUtil.fixNull(request.getParameter("action"));
  String username = HushUtil.fixNull(request.getParameter("username"));
  String firstName = HushUtil.fixNull(request.getParameter("firstName"));
  String lastName = HushUtil.fixNull(request.getParameter("lastName"));
  String email = HushUtil.fixNull(request.getParameter("email"));
  String password = HushUtil.fixNull(request.getParameter("password"));
  String confirmPassword = HushUtil.fixNull(request.getParameter("confirmPassword"));
  BitSet errors = new BitSet(10);

  if (action.equalsIgnoreCase("create")) {
    // check for form errors
    errors.set(0, password.length() > 0
      && !password.equals(confirmPassword));
    errors.set(1, username.length() == 0);
    errors.set(2, firstName.length() == 0);
    errors.set(3, lastName.length() == 0);
    errors.set(4, email.length() == 0);
    errors.set(5, password.length() == 0);
    errors.set(6, confirmPassword.length() == 0);
    if (errors.isEmpty()) {
      UserManager um = ResourceManager.getInstance().getUserManager();
      um.createUser(username, firstName, lastName, email, password, "user");
      response.sendRedirect("/user");
      return;
    }
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
<div class="main">
  <jsp:include page="/include/error.jsp"/>
  <div id="stylized" class="myform">
    <form id="form" method="post" name="form" action="j_security_check">
      <h1>Existing users</h1>

      <p>Enter your credentials</p>
      <label>Username</label>
      <input type="text" name="j_username" id="username"/>

      <label>Password</label>
      <input type="password" name="j_password" id="password"/>

      <button type="submit">Log In</button>
      <div class="spacer"></div>
    </form>
    <form id="form" method="post" name="form" action="/signup.jsp">
      <h1>New Users</h1>

      <p>Register by filling the form below</p>

      <label>Username
        <span class="small">Your login</span>
        <%
          if (errors.get(1)) {
        %>
        <span class="error">*required</span>
        <%
          }
        %>
      </label>
      <input type="text" name="username" id="username"
             value="<%= username %>"/>

      <label>First Name
        <span class="small"></span>
        <%
          if (errors.get(2)) {
        %>
        <span class="error">*required</span>
        <%
          }
        %>
      </label>
      <input type="text" name="firstName" id="firstName"
             value="<%=firstName%>"/>

      <label>Last Name
        <span class="small"></span>
        <%
          if (errors.get(3)) {
        %>
        <span class="error">*required</span>
        <%
          }
        %>
      </label>
      <input type="text" name="lastName" id="lastName"
             value="<%=lastName%>"/>

      <label>Email
        <span class="small"></span>
        <%
          if (errors.get(4)) {
        %>
        <span class="error">*required</span>
        <%
          }
        %>
      </label>
      <input type="text" name="email" id="email"
             value="<%=email%>"/>

      <label>Password
        <span class="small">Make it good!</span>
        <%
          if (errors.get(5)) {
        %>
        <span class="error">Please enter a password.</span>
        <%
          }
        %>
      </label>
      <input type="password" name="password" id="password"/>

      <label>Confirm Password
        <span class="small"></span>
        <%
          if (errors.get(0)) {
        %>
        <span class="error">password mismatch</span>
        <%
          }
        %>
      </label>
      <input type="password" name="confirmPassword" id="confirmPassword"/>


      <input type="hidden" name="action" value="create"/>
      <button name="submit" type="submit">Sign Up</button>
      <div class="spacer"></div>
    </form>
  </div>
</div>
</body>
</html>