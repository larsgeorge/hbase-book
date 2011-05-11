<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="com.hbasebook.hush.Counters" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.NavigableMap" %>
<%@ page import="java.util.Calendar" %>
<%@ page import="com.hbasebook.hush.model.Counter" %>
<%@ page import="com.hbasebook.hush.model.ShortUrl" %>
<%@ page import="com.hbasebook.hush.model.ShortUrlStatistics" %>
<%@ page import="com.hbasebook.hush.model.User" %>
<%@ page import="java.util.*" %>
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
  ShortUrl url = manager.getUrlManager().getShortUrl(shortId);
  ShortUrlStatistics urlStats = manager.getCounters().getDailyStatistics(
    url);
  if (urlStats == null) {
    request.getRequestDispatcher("/error.jsp?error=Nothing+to+report!+" +
      "Click+on+the+short+URL+first.").
      forward(request, response);
  }
  SimpleDateFormat formatter = new SimpleDateFormat("yyyy, MM, dd");
  String qrUrl = url.toString() + ".q";
  ShortUrl aggUrl = manager.getUrlManager().getShortUrl(url.getRefShortId());
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
  "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Short URL Details</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<div class="wrap">
<jsp:include page="/include/header.jsp"/>
<div class="main">
  <div id="summary">
    <div id="info">
	  <h2><%= url %></h2>
	  <p class="longUrl">Target: <a href="<%= url.getLongUrl() %>"><%= url.getLongUrl() %></a></p>
	  <p><%= url.getClicks() %> clicks on this link (<%= url.getId() %>
	    created by <%= User.displayName (url.getUser()) %>)</p>
<%
   if (aggUrl != null) {
      String detailsLink = aggUrl.toString() + "+";
%>
	  <p><%= aggUrl.getClicks() %> clicks on aggregate link
	    (<a href="<%= detailsLink %>"><%= aggUrl.getId() %></a>
	    created by <%= User.displayName (aggUrl.getUser()) %>)</p>
<% } %>
    </div>
    <div id="code">
      <img src="<%= qrUrl %>" width="75" height="75" alt="<%= qrUrl %>" />
    </div>
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
         NavigableSet<?> clicks = urlStats.getCounters("clicks").descendingSet();
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
        var chart = new google.visualization.AnnotatedTimeLine(
          document.getElementById("div_for_timeline"));
        chart.draw(data, {zoomStartTime: new Date(<%= formatter.format(zoomStart)%>),
          zoomEndTime: new Date(<%= formatter.format(lastDate)%>)});

//        google.visualization.events.addListener(chart, "rangechange", rangeChangeHandler);
//        function rangeChangeHandler(e) {
//          alert("Start: " + e.start + ", End: " + e.end);
//        }
      }
    </script>
    <div id="div_for_timeline" style="width: 620px; height: 280px;"></div>
  </div>
  <br/><br/>
  <div id="country_chart">
    <h3>Clicks by Country</h3>
    <%
      // get details for countries
      NavigableSet<?> clicksByCountry = urlStats.getCounters("clicksbycountry");
      // the schema maps in the table and column schema, plus key formats etc.
      // StatisticsSelector ss = manager.getStatisticsSelector("url",
      //   StatisticsSelector.Granularity.DAILY, url); // schema, granularity, and value (optional)
      // using a value narrows down the selected data -> performance
      // List<Counter> counters = ss.getCounters("/clicks[between(startDate, endDate)]/country/@ISOCode");
      StringBuffer data = new StringBuffer();
      StringBuffer label = new StringBuffer();
      StringBuffer legend = new StringBuffer();
      long min = 0, max = 0;
      for (Object obj: clicksByCountry) {
        Counter<String, Long> counter = (Counter<String, Long>) obj;
        if (data.length() > 0) {
          data.append(",");
          label.append("|");
          legend.append("|");
        }
        data.append(counter.getValue());
        String country = new Locale("", counter.getKey()).getDisplayCountry();
        label.append(counter.getValue());
        legend.append(country);
        min = Math.min(min, counter.getValue().longValue());
        max = Math.max(max, counter.getValue().longValue());
      }
      // or use http://code.google.com/apis/visualization/documentation/gallery/piechart.html
    %>
    <img src="http://chart.apis.google.com/chart?chs=600x300&cht=p&chco=3399CC&chd=t:<%= data %>&chl=<%= label%>&chdl=<%= legend%>&chds=<%=min%>,<%=max%>"
         width="600" height="300" alt="" />
  </div>
</div>
</div>
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
