package admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

// cc ClusterStatusExample Example reporting the status of a cluster
public class ClusterStatusExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    // vv ClusterStatusExample
    ClusterStatus status = admin.getClusterStatus(); // co ClusterStatusExample-1-GetStatus Get the cluster status.

    System.out.println("Cluster Status:\n--------------");
    System.out.println("HBase Version: " + status.getHBaseVersion());
    System.out.println("Version: " + status.getVersion());
    System.out.println("Cluster ID: " + status.getClusterId());
    System.out.println("Master: " + status.getMaster());
    System.out.println("No. Backup Masters: " +
      status.getBackupMastersSize());
    System.out.println("Backup Masters: " + status.getBackupMasters());

    System.out.println("No. Live Servers: " + status.getServersSize());
    System.out.println("Servers: " + status.getServers());
    System.out.println("No. Dead Servers: " + status.getDeadServers());
    System.out.println("Dead Servers: " + status.getDeadServerNames());
    System.out.println("No. Regions: " + status.getRegionsCount());
    System.out.println("Regions in Transition: " +
      status.getRegionsInTransition());
    System.out.println("No. Requests: " + status.getRequestsCount());
    System.out.println("Avg Load: " + status.getAverageLoad());
    System.out.println("Balancer On: " + status.getBalancerOn());
    System.out.println("Is Balancer On: " + status.isBalancerOn());
    System.out.println("Master Coprocessors: " +
      Arrays.asList(status.getMasterCoprocessors()));

    System.out.println("\nServer Info:\n--------------");
    for (ServerName server : status.getServers()) { // co ClusterStatusExample-2-ServerInfo Iterate over the included server instances.
      System.out.println("Hostname: " + server.getHostname());
      System.out.println("Host and Port: " + server.getHostAndPort());
      System.out.println("Server Name: " + server.getServerName());
      System.out.println("RPC Port: " + server.getPort());
      System.out.println("Start Code: " + server.getStartcode());

      ServerLoad load = status.getLoad(server); // co ClusterStatusExample-3-ServerLoad Retrieve the load details for the current server.

      System.out.println("\nServer Load:\n--------------");
      System.out.println("Info Port: " + load.getInfoServerPort());
      System.out.println("Load: " + load.getLoad());
      System.out.println("Max Heap (MB): " + load.getMaxHeapMB());
      System.out.println("Used Heap (MB): " + load.getUsedHeapMB());
      System.out.println("Memstore Size (MB): " +
        load.getMemstoreSizeInMB());
      System.out.println("No. Regions: " + load.getNumberOfRegions());
      System.out.println("No. Requests: " + load.getNumberOfRequests());
      System.out.println("Total No. Requests: " +
        load.getTotalNumberOfRequests());
      System.out.println("No. Requests per Sec: " +
        load.getRequestsPerSecond());
      System.out.println("No. Read Requests: " +
        load.getReadRequestsCount());
      System.out.println("No. Write Requests: " +
        load.getWriteRequestsCount());
      System.out.println("No. Stores: " + load.getStores());
      System.out.println("Store Size Uncompressed (MB): " +
        load.getStoreUncompressedSizeMB());
      System.out.println("No. Storefiles: " + load.getStorefiles());
      System.out.println("Storefile Size (MB): " +
        load.getStorefileSizeInMB());
      System.out.println("Storefile Index Size (MB): " +
        load.getStorefileIndexSizeInMB());
      System.out.println("Root Index Size: " + load.getRootIndexSizeKB());
      System.out.println("Total Bloom Size: " +
        load.getTotalStaticBloomSizeKB());
      System.out.println("Total Index Size: " +
        load.getTotalStaticIndexSizeKB());
      System.out.println("Current Compacted Cells: " +
        load.getCurrentCompactedKVs());
      System.out.println("Total Compacting Cells: " +
        load.getTotalCompactingKVs());
      System.out.println("Coprocessors1: " +
        Arrays.asList(load.getRegionServerCoprocessors()));
      System.out.println("Coprocessors2: " +
        Arrays.asList(load.getRsCoprocessors()));
      System.out.println("Replication Load Sink: " +
        load.getReplicationLoadSink());
      System.out.println("Replication Load Source: " +
        load.getReplicationLoadSourceList());

      System.out.println("\nRegion Load:\n--------------");
      for (Map.Entry<byte[], RegionLoad> entry : // co ClusterStatusExample-4-Regions Iterate over the region details of the current server.
          load.getRegionsLoad().entrySet()) {
        System.out.println("Region: " + Bytes.toStringBinary(entry.getKey()));

        RegionLoad regionLoad = entry.getValue(); // co ClusterStatusExample-5-RegionLoad Get the load details for the current region.

        System.out.println("Name: " + Bytes.toStringBinary(
          regionLoad.getName()));
        System.out.println("Name (as String): " +
          regionLoad.getNameAsString());
        System.out.println("No. Requests: " + regionLoad.getRequestsCount());
        System.out.println("No. Read Requests: " +
          regionLoad.getReadRequestsCount());
        System.out.println("No. Write Requests: " +
          regionLoad.getWriteRequestsCount());
        System.out.println("No. Stores: " + regionLoad.getStores());
        System.out.println("No. Storefiles: " + regionLoad.getStorefiles());
        System.out.println("Data Locality: " + regionLoad.getDataLocality());
        System.out.println("Storefile Size (MB): " +
          regionLoad.getStorefileSizeMB());
        System.out.println("Storefile Index Size (MB): " +
          regionLoad.getStorefileIndexSizeMB());
        System.out.println("Memstore Size (MB): " +
          regionLoad.getMemStoreSizeMB());
        System.out.println("Root Index Size: " +
          regionLoad.getRootIndexSizeKB());
        System.out.println("Total Bloom Size: " +
          regionLoad.getTotalStaticBloomSizeKB());
        System.out.println("Total Index Size: " +
          regionLoad.getTotalStaticIndexSizeKB());
        System.out.println("Current Compacted Cells: " +
          regionLoad.getCurrentCompactedKVs());
        System.out.println("Total Compacting Cells: " +
          regionLoad.getTotalCompactingKVs());
        System.out.println();
      }
    }
    // ^^ ClusterStatusExample
  }
}
