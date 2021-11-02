package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IDomainResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.IResourceProvider;

public abstract class BaseProxy implements IResourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(BaseProxy.class);

    protected TableName tableName;
    
    @Inject
    protected HBaseConnector connector;
    
    FhirContext ctx = FhirContext.forR4();

    protected Connection getConnection() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        return connector.getConnection();
    }
    
    protected void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

//    protected abstract StoreAuditOutcomeEnum saveToDatabase(IDomainResource resouce) throws Exception;

    protected void writeToFileSystem(String fileName, String json) throws IOException {
        Configuration configuration = new Configuration();
        String clusterIP = (System.getenv("CLUSTER_IP"));
        configuration.set("fs.defaultFS", "hdfs://" + clusterIP + ":8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsWritePath = new Path("/data/pegacorn/sample-dataset/" + fileName + ".json");

        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);

        // Set replication
        fileSystem.setReplication(hdfsWritePath, (short) 2);

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(json.toString());
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }
    

    protected void save(Put row) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (!getConnection().getAdmin().tableExists(tableName)) {
            createTable();
        }
        Table table = getConnection().getTable(tableName);
        table.put(row);
        LOG.debug("Save successful. Id: " + Bytes.toString(row.getRow()));
        table.close();
    }

    protected void save(List<Put> rows) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (!getConnection().getAdmin().tableExists(tableName)) {
            createTable();
        }
        Table table = getConnection().getTable(tableName);
        table.put(rows);
        LOG.debug("Save successful.");
        table.close();
    }
    
    protected abstract void initialiseTableName();
    protected abstract void createTable() throws IOException;

    protected String parseResourceToJsonString(IDomainResource resource) {
        IParser parser = ctx.newJsonParser();
        String parsedResource = parser.encodeResourceToString(resource);
        return parsedResource;
    }

    protected IBaseResource parseResourceFromJsonString(String json) {
        IParser parser = ctx.newJsonParser();
        IBaseResource parsedResource = parser.parseResource(json);
        return parsedResource;
    }
}
