<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.table.UserTable" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.UserManager" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="org.apache.hadoop.hbase.client.Put" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="java.util.BitSet" %>
<%
  String action = request.getParameter("action");
  String userName = request.getParameter("username");
  if (userName == null)
    userName = "";
  String firstName = request.getParameter("firstName");
  if (firstName == null)
    firstName = "";
  String lastName = request.getParameter("lastName");
  if (lastName == null)
    lastName = "";
  String email = request.getParameter("email");
  if (email == null)
    email = "";
  String password = request.getParameter("password");
  if (password == null)
    password = "";
  String confirmPassword = request.getParameter("confirmPassword");
  if (confirmPassword == null)
    confirmPassword = "";
  BitSet errors = new BitSet(10);
  
  if (action != null && action.equalsIgnoreCase("create")) {
    // check for form errors
    errors.set(0, password.length() > 0
        && !password.equals(confirmPassword));
    errors.set(1, userName.length() == 0);
    errors.set(2, firstName.length() == 0);
    errors.set(3, lastName.length() == 0);
    errors.set(4, email.length() == 0);
    errors.set(5, password.length() == 0);
    errors.set(6, confirmPassword.length() == 0);
    if (errors.isEmpty()) {
      UserManager um = ResourceManager.getInstance().getUserManager();
      um.createUser(userName, firstName, lastName, email, password);
      response.sendRedirect("/user");
      return ;
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
             value="<%=userName%>"/>

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