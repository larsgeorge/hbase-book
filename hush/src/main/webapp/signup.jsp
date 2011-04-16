<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.table.UserTable" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="org.apache.hadoop.hbase.client.Put" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="java.util.BitSet" %>
<%
  String submit = request.getParameter("submit");
  String userName = request.getParameter("username");
  if (userName == null) userName = "";
  String firstName = request.getParameter("firstName");
  if (firstName == null) firstName = "";
  String lastName = request.getParameter("lastName");
  if (lastName == null) lastName = "";
  String email = request.getParameter("email");
  if (email == null) email = "";
  String password = request.getParameter("password");
  if (password == null) password = "";
  String confirmPassword = request.getParameter("confirmPassword");
  if (confirmPassword == null) confirmPassword = "";
  BitSet errors = new BitSet(10);
  boolean success = false;
  if (submit != null) {
    // check for form errors
    errors.set(0, password.length() > 0 && !password.equals(confirmPassword));
    errors.set(1, userName.length() == 0);
    errors.set(2, firstName.length() == 0);
    errors.set(3, lastName.length() == 0);
    errors.set(4, email.length() == 0);
    errors.set(5, password.length() == 0);
    errors.set(6, confirmPassword.length() == 0);
    if (errors.isEmpty()) {
      ResourceManager manager = ResourceManager.getInstance();
      HTable table = manager.getTable(UserTable.NAME);
      Put put = new Put(Bytes.toBytes(userName));
      put.add(UserTable.DATA_FAMILY, UserTable.FIRSTNAME,
        Bytes.toBytes(firstName));
      put.add(UserTable.DATA_FAMILY, UserTable.LASTNAME,
        Bytes.toBytes(lastName));
      put.add(UserTable.DATA_FAMILY, UserTable.EMAIL,
        Bytes.toBytes(email));
      put.add(UserTable.DATA_FAMILY, UserTable.CREDENTIALS,
        Bytes.toBytes(password));
      put.add(UserTable.DATA_FAMILY, UserTable.ROLES,
        UserTable.USER_ROLE);
      table.put(put);
      table.flushCommits();
      manager.putTable(table);
      success = true;
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
<% if (!success) { %>
<div id="stylized" class="myform">
  <form id="form" method="post" name="form" action="/signup.jsp">
    <h1>Sign up</h1>

    <p>Enter your information below</p>

    <label>Username
      <span class="small">Your login</span>
      <% if (errors.get(1)) { %>
      <span class="error">*required</span>
      <% } %>
    </label>
    <input type="text" name="username" id="username"
           value="<%= userName%>"/>

    <label>First Name
      <span class="small"></span>
      <% if (errors.get(2)) { %>
      <span class="error">*required</span>
      <% } %>
    </label>
    <input type="text" name="firstName" id="firstName"
           value="<%= firstName%>"/>

    <label>Last Name
      <span class="small"></span>
      <% if (errors.get(3)) { %>
      <span class="error">*required</span>
      <% } %>
    </label>
    <input type="text" name="lastName" id="lastName"
           value="<%= lastName%>"/>

    <label>Email
      <span class="small"></span>
      <% if (errors.get(4)) { %>
      <span class="error">*required</span>
      <% } %>
    </label>
    <input type="text" name="email" id="email"
           value="<%= email%>"/>

    <label>Password
      <span class="small">Make it good!</span>
      <% if (errors.get(5)) { %>
      <span class="error">Please enter a password.</span>
      <% } %>
    </label>
    <input type="password" name="password" id="password"/>

    <label>Confirm Password
      <span class="small"></span>
      <% if (errors.get(0)) { %>
      <span class="error">password mismatch</span>
      <% } %>
    </label>
    <input type="password" name="confirmPassword" id="confirmPassword"/>

    <button name="submit" type="submit">Sign Up</button>
    <div class="spacer"></div>
  </form>
</div>
<% } else { %>
  <h1>Welcome <%= firstName%>!</h1>

  <p>Thank you for singing up! You will be redirected to your account page...</p>
  <p>(Click <a href="/user">here</a> if this takes for more than 5 seconds)</p>
<script type="text/javascript">
  function Redirect() {
    location.href = "/user";
  }
  setTimeout('Redirect()', 4000);
</script>
<% } %>
</body>
</html>
