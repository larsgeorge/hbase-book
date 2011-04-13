<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head><title>Login</title></head>
<body>
<h2>Login</h2>

<form action="j_security_check">
  <span>
	<label>Username</label>
	<input type="text" name="j_username" />
  </span>
  <span>
    <label>Password</label>
    <input type="password" name="j_password" />
  </span>
  <span>
    <input type="submit" name="log in">
  </span>
</form>
</body>
</html>
