<%@ page import="com.hbasebook.hush.ResourceManager" %>
<%@ page import="org.apache.hadoop.hbase.client.HTable" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.client.Scan" %>
<%@ page import="com.hbasebook.hush.table.UserShortUrlTable" %>
<%@ page import="java.security.Principal" %>
<%@ page import="org.apache.hadoop.hbase.client.ResultScanner" %>
<%@ page import="org.apache.hadoop.hbase.client.Result" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="org.apache.hadoop.hbase.client.Get" %>
<%@ page import="org.apache.hadoop.hbase.client.Row" %>
<%@ page import="com.hbasebook.hush.table.ShortUrlTable" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<%
  Principal principal = request.getUserPrincipal();
  String userName = principal.getName();
  ResourceManager manager = ResourceManager.getInstance();
  HTable userShortUrltable = manager.getTable(UserShortUrlTable.NAME);
  HTable shortUrltable = manager.getTable(ShortUrlTable.NAME);

  byte[] startRow = Bytes.toBytes(userName);
  byte[] one = new byte[]{1};
  byte[] stopRow = Bytes.add(startRow, one);

  Scan scan = new Scan(startRow, stopRow);
  scan.addFamily(UserShortUrlTable.DAILY_FAMILY);

  ResultScanner scanner = userShortUrltable.getScanner(scan);
  List<Row> gets = new ArrayList<Row>();
  for (Result result : scanner) {
    String rowKey = Bytes.toString(result.getRow());
    String shortId = rowKey.substring(rowKey.indexOf(0) + 1);
    Get get = new Get(Bytes.toBytes(shortId));
    gets.add(get);
  }
  Object[] getResults = shortUrltable.batch(gets);
  manager.putTable(userShortUrltable);
  manager.putTable(shortUrltable);
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
  <title>Account Details</title>
  <link href="/style.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<jsp:include page="/include/header.jsp"/>
<div class="main">
  <h2>My Links</h2>

  <p>
  <ul>
    <%
      for (Object object : getResults) {
        if (object instanceof Result) {
          Result result = (Result) object;
    %>
    <li>
      Short Id: <%= Bytes.toString(result.getRow()) %>
      URL: <%= Bytes.toString(result.getValue(ShortUrlTable.DATA_FAMILY, ShortUrlTable.URL)) %>
    </li>
  <%--
Sparkline:
http://chart.apis.google.com/chart?cht=ls&chs=120x15&chd=t:1.72278429785512,1.68653523732439,2.13678672602185,
1.90720934266058,1.33803549748511,5.92131583511592,2.45158119905186,1.89957796149621,1.90339365207839,2.01277678210094,
1.88240735387639,1.73295947274094,12.4709487194311,6.05995259293519,3.33554951725733,1.94282245476094,7.58813667109903,
4.91524541828063,3.39914436029369,7.97797305891195,5.10412210209863,3.97340579291207,2.86939931780077,25.9867607099497,
110.0,57.5139041452275,75.6759553679829,57.1914782910331,37.0999595305544,24.5425218245939&chco=999999&chm=B,999999,
0,0,0&chds=0,120

Timeline:
    <div id="timeline_chart" class="left">
      <h3>Hot Right Now: <a href="/page/Dominique_Dawes">Dominique Dawes</a> </h3>
      <script type="text/javascript" src="http://www.google.com/jsapi"></script><script type="text/javascript">
    google.load("visualization", "1", {packages:["annotatedtimeline"]});
    google.setOnLoadCallback(drawChart);
    function drawChart(){
      var data = new google.visualization.DataTable();
      data.addColumn('date', 'Date');
data.addColumn('number', 'Wikipedia Page Views');
data.addRows(180);
data.setValue(0, 0, new Date(2009, 10, 1));
data.setValue(0, 1, 126);
data.setValue(1, 0, new Date(2009, 10, 2));
data.setValue(1, 1, 163);
data.setValue(2, 0, new Date(2009, 10, 3));
data.setValue(2, 1, 199);
data.setValue(3, 0, new Date(2009, 10, 4));
data.setValue(3, 1, 294);
...

data.setValue(178, 0, new Date(2010, 3, 29));
data.setValue(178, 1, 3046);
data.setValue(179, 0, new Date(2010, 3, 30));
data.setValue(179, 1, 23751);

      var chart = new google.visualization.AnnotatedTimeLine(document.getElementById('div_id_to_create'));
      chart.draw(data, {zoomEndTime: new Date(2010, 3, 30), zoomStartTime: new Date(2010, 3, 1)});
    }
    </script><div id="div_id_to_create" style="width: 620px; height: 280px;"></div>



  	</div> <!-- #timeline_chart -->
    --%>

    <%
    } else {
    %>
    ERROR
    <%
        }
      }
    %>
  </ul>
  </p>
</div>
<jsp:include page="/include/footer.jsp"/>
</body>
</html>
