<%@ page contentType="text/html;charset=UTF-8"%>
<%@ page import="java.util.List"%>
<%@ page import="com.hbasebook.hush.table.ShortDomain"%>
<%@ page import="com.hbasebook.hush.ResourceManager"%>
<%@ page import="com.hbasebook.hush.DomainManager"%>
<%
  DomainManager dm = ResourceManager.getInstance().getDomainManager();

  try {
    String action = request.getParameter("action");
    if (action != null && action.equalsIgnoreCase("add")) {
      String ldom = request.getParameter("ldom");
      String sdom = request.getParameter("sdom");
      dm.addLongDomain(sdom, ldom);
    } else if (action != null && action.equalsIgnoreCase("delete")) {
      String ldom = request.getParameter("ldom");
      dm.deleteLongDomain(ldom);
    }
  } catch (Exception e) {
    request.setAttribute("error", e.getMessage());
  }

  List<ShortDomain> list = dm.listShortDomains();
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
<title>Domains</title>
<link href="/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
<jsp:include page="/include/header.jsp" />
<div class="main">
<h2>Domains</h2>
<jsp:include page="/include/error.jsp" />

<table id="domains">
	<thead>
		<tr>
			<td>Short Domain</td>
			<td>Original Domain</td>
		</tr>
	</thead>
	<tbody>
		<%
		  for (ShortDomain sdom : list) {
		    List<String> ldoms = sdom.getDomains();
		%>
		<tr>
			<td class="shortDomain"><%=sdom.getShortDomain()%></td>
			<td>
			<table style="border: none; margin: 0; padding: 0;">
				<%
				  for (String ldom : ldoms) {
				%>
				<tr>
					<td style="border: none; margin: 0; padding: 0;" class="longDomain"><%=ldom%></td>
					<td style="border: none; margin: 0; padding: 0;" class="actions">[
					<a href="/admin/domains.jsp?action=delete&ldom=<%=ldom%>">delete</a>
					]</td>
				</tr>
				<%
				  }
				%>
			</table>
			</td>
		</tr>
		<%
		  }
		%>
		<tr>		
			<td colspan="2">
			<form action="/admin/domains.jsp"><input type="hidden"
				name="action" value="add" />
			<p>Shorten <input type="text" size="40"
				name="ldom" /> as <input type="text" size="15" name="sdom" /> <input
				type="submit" value="Add Domain Mapping" /></p>
			</form>
			</td>
		</tr>
	</tbody>
</table>

</div>
<jsp:include page="/include/footer.jsp" />
</body>
</html>