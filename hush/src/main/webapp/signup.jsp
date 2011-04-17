<%
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
   "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Sign up</title>
  <link href="/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
<div class="main">
<jsp:include page="/include/error.jsp"></jsp:include>

<div id="stylized" class="myform">

<form id="form" method="post" name="form" action="j_security_check" >
<h1>Existing users</h1>
<p>Enter your credentials</p>
  <label>Username</label>
  <input type="text" name="j_username" id="username" />
  
  <label>Password</label>
  <input type="password"  name="j_password" id="password" />
  
  <button type="submit">Log In</button>
  <div class="spacer"></div>
</form> 
<form id="form" method="post" name="form" action="/signup.jsp" >
<h1>New users</h1>
<p>Register by filling the form below</p>
  <label>Username</label>
  <input type="text" name="username" id="username" />
  
  <label>E-mail
	<span class="small">
	  Help us spam you
	</span>
  </label>
  <input type="text" name="email" id="email" />
  
  <label>Password
	<span class="small">
	  Make it good!
	</span>
  </label>
  <input type="password"  name="password" id="password" />
  
  <label>Confirm Password</label>
  <input type="password"  name="confirmPassword" id="confirmPassword" />
  
  <button type="submit">Sign Up</button>
  <div class="spacer"></div>
</form> 

</div>

</div>
</body>
</html>
