<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
   "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Sign up</title>
  <link href="/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
<div id="stylized" class="myform">
<form id="form" method="post" name="form" action="/signup.jsp" >
<h1>Sign up</h1>
<p>Enter your information below</p>
  <label>Username
	<span class="small">
	  Your login
	</span>
  </label>
  <input type="text" name="username" id="username" />
  <label>Password
	<span class="small">
	  Make it good!
	</span>
  </label>
  <input type="password"  name="password" id="password" />
  <label>Confirm Password
	<span class="small">
	  
	</span>
  </label>
  <input type="password"  name="confirmPassword" id="confirmPassword" />
  <button type="submit">Sign Up</button>
  <div class="spacer"></div>
</form> 
</div>
</body>
</html>
