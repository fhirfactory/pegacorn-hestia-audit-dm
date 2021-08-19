package net.fhirfactory.pegacorn.hestia.audit.dm;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class HBaseConnector {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnector.class);

    protected static Connection connection = null;
    
   
    
    public Connection getConnection() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if(connection == null) {
            LOG.info("No configuration found. Creating a new one");
            Configuration config = HBaseConfiguration.create();

            String zooKeeperIP = (System.getenv("ZOOKEEPER_CLUSTER_IP"));
            config.set("hbase.zookeeper.quorum", zooKeeperIP);
            config.set("hbase.zookeeper.property.clientPort", "2181");

            connection = ConnectionFactory.createConnection(config);
        }
        return connection;
    }
    

}
