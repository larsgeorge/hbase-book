<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.Counters" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.NavigableMap" %>
<%@ page import="java.util.Calendar" %>
<%@ page import="com.hbasebook.hush.model.ShortUrl" %>
<%@ page import="com.hbasebook.hush.model.ShortUrlStatistics" %>
<%@ page import="java.util.*" %>
<%@ page import="com.hbasebook.hush.Counter" %>
<%
  // check if the parameter was given
  String shortId = request.getParameter("sid");
  if (shortId == null) {
    shortId = (String) request.getAttribute("sid");
  }
  if (shortId == null) {
    request.getRequestDispatcher("/error.jsp?error=No+short+ID+provided!").
      forward(request, response);
  }
  // get statistics
  ResourceManager manager = ResourceManager.getInstance();
  ShortUrl shortUrl = manager.getUrlManager().getShortUrl(shortId);
  ShortUrlStatistics stats = manager.getCounters().getDailyStatistics(
    shortUrl);
  if (stats == null) {
    request.getRequestDispatcher("/error.jsp?error=Nothing+found!").
      forward(request, response);
  }
  SimpleDateFormat formatter = new SimpleDateFormat("yyyy, MM, dd");
  String longUrl = shortUrl.getLongUrl();
  String user = shortUrl.getDisplayUser();
  String qrUrl = shortUrl.toString() + ".q";
  
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
  "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Short URL Details</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<jsp:include page="/include/header.jsp"/>
<div class="main">
  <h2>Details on <%= shortUrl %></h2>

  <div id="summary">
    <p><a href="<%= longUrl %>"><%= longUrl %></a> created by <%= user %></p>
    <img src="<%= qrUrl %>"
     width="60" height="60" alt="<%= qrUrl %>" />    
  </div>

  <div id="timeline_chart">
    <h3>Clicks by Date</h3>
    <script type="text/javascript" src="http://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["annotatedtimeline"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = new google.visualization.DataTable();
        data.addColumn('date', 'Date');
        data.addColumn('number', 'URL Clicks');
      <%
         NavigableSet<?> clicks = stats.getCounters("clicks").descendingSet();
      %>
        data.addRows(<%= clicks.size()%>);
      <%
         int row = 0;
         Date firstDate = null;
         Date lastDate = null;
         for (Object obj : clicks) {
           Counter<Date, Double> counter = (Counter<Date, Double>) obj;
           Date date = counter.getKey();
           String jsDate = formatter.format(date);
      %>
        data.setValue(<%= row%>, 0, new Date(<%= jsDate%>));
        data.setValue(<%= row%>, 1, <%= counter.getValue()%>);
      <%
           row++;
           if (firstDate == null) firstDate = date;
           lastDate = date;
         }
         Calendar cal = Calendar.getInstance();
         cal.setTime(lastDate);
         cal.add(Calendar.MONTH, -1);
         Date zoomStart = cal.getTime();
         if (zoomStart.before(firstDate)) zoomStart = firstDate;
      %>
        var chart = new google.visualization.AnnotatedTimeLine(document.getElementById('div_for_timeline'));
        chart.draw(data, {zoomStartTime: new Date(<%= formatter.format(zoomStart)%>),
          zoomEndTime: new Date(<%= formatter.format(lastDate)%>)});
      }
    </script>
    <div id="div_for_timeline" style="width: 620px; height: 280px;"></div>
  </div>
  <div id="country_chart">
    <h3>Clicks by Country</h3>
    <%
      // get details for countries
      NavigableSet<?> clicksByCountry = stats.getCounters("clicksbycountry");
      StringBuffer data = new StringBuffer();
      for (Object obj: clicksByCountry) {
        Counter<String, Long> counter = (Counter<String, Long>) obj;
        if (data.length() > 0) {
          data.append(",");
        }
        data.append(counter.getValue());
      }
      // or use http://code.google.com/apis/visualization/documentation/gallery/piechart.html
    %>
    <img src="http://chart.apis.google.com/chart?chs=300x265&cht=p&chco=3399CC&chd=t:<%= data %>"
         width="150" height="133" alt="" />
  </div>
</div>
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
