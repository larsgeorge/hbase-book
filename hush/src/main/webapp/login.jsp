<%@ page contentType="text/html;charset=UTF-8" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
   "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Login</title>
  <link href="/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
<div id="stylized" class="myform">
<form id="form" method="post" name="form" action="j_security_check" >
<h1>Login</h1>
<p></p>
  <label>Username
	<span class="small">
	  Your login
	</span>
  </label>
  <input type="text" name="j_username" id="username" />
  <label>Password
	<span class="small">
	</span>
  </label>
  <input type="password"  name="j_password" id="password" />
  <button type="submit">Log In</button>
  <div class="spacer"></div>
</form> 
</div>
</body>
</html>


