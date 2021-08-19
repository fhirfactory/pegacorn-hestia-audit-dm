package net.fhirfactory.pegacorn.hestia.audit.dm;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IDomainResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.IResourceProvider;


public abstract class AuditBaseProxy implements IResourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AuditBaseProxy.class);
    
    @Inject
    private HBaseConnector connector;
    
    //TODO Note this will eventually have an enum for when the server is down
    protected Connection getConnection() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        return connector.getConnection();
    }
    
    protected abstract void saveToDatabase(IDomainResource resouce) throws Exception;
    
    
    protected void writeToFileSystem(String fileName, String json) throws IOException {
        Configuration configuration = new Configuration();
        String clusterIP = (System.getenv("CLUSTER_IP"));
      configuration.set("fs.defaultFS", "hdfs://"+clusterIP+":8020");
      FileSystem fileSystem = FileSystem.get(configuration);
      Path hdfsWritePath = new Path("/data/pegacorn/sample-dataset/" + fileName + ".json");
      
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        
        // Set replication
        fileSystem.setReplication(hdfsWritePath, (short) 2);

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        bufferedWriter.write(json.toString());
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }
    
    
    protected String parseResourceToJsonString(IDomainResource resource) {
        FhirContext ctx = FhirContext.forR4();

        IParser parser = ctx.newJsonParser();
        String parsedResource = parser.encodeResourceToString(resource);
        
        return parsedResource;
    }
    
    protected IBaseResource parseResourceFromJsonString(String json) {
        FhirContext ctx = FhirContext.forR4();

        IParser parser = ctx.newJsonParser();
        IBaseResource parsedResource = parser.parseResource(json);
        
        return parsedResource;
    }
}
