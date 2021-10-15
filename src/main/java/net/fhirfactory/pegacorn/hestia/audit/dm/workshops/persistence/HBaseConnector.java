/*
 * Copyright (c) 2021 Kelly Skye
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence;

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
        LOG.info("HBaseConnector:getConnection() - Entry");
        if (connection == null) {
            LOG.info("No configuration found. Creating a new one");
            Configuration config = HBaseConfiguration.create();

            String zookeeperIP = (System.getenv("ZOOKEEPER_CLUSTER_IP"));
            config.set("hbase.zookeeper.quorum", zookeeperIP); 
            String zookeeperPort = (System.getenv("ZOOKEEPER_CLUSTER_PORT"));
            config.set("hbase.zookeeper.property.clientPort", zookeeperPort); //"17306"
            //XXX First connection always throws caught error. Fine on 2nd attempt. Inelegant solution.
            try {
                connection = ConnectionFactory.createConnection(config);
            } catch (NoSuchMethodError e) {
                LOG.error("Error on connection creation. Retrying... " + e.getMessage());
                connection = ConnectionFactory.createConnection(config);
            }
        }
        LOG.info("HBaseConnector:getConnection() - Exit");
        return connection;
    }

}
