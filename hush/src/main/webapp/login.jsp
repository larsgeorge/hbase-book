<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Log in</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<div class="main">
  <div id="stylized" class="myform">
    <form id="form" method="post" name="form" action="j_security_check">
      <h1>Log in</h1>

      <p>New users <a href="/signup.jsp">sign up here</a>.</p>
      <label>Username</label>
      <input type="text" name="j_username" id="username"/>

      <label>Password</label>
      <input type="password" name="j_password" id="password"/>

      <button type="submit">Log In</button>
      <div class="spacer"></div>
    </form>
  </div>
</div>
</body>
</html>