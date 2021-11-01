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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.task.common;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IDomainResource;
import org.hl7.fhir.r4.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.IResourceProvider;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.HBaseConnector;

public abstract class TaskBaseProxy implements IResourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TaskBaseProxy.class);

    protected static final TableName TABLE_NAME = TableName.valueOf("TASK");
    protected static final byte[] CF1 = Bytes.toBytes("INFO");
    protected static final byte[] CF2 = Bytes.toBytes("DATA");
    //partOf, basedOn, code, status, location, owner, focus
    protected static final byte[] Q_STATUS = Bytes.toBytes("STATUS"); //status.definition
    protected static final byte[] Q_LOCATION = Bytes.toBytes("LOC"); //location.reference
    protected static final byte[] Q_CODE = Bytes.toBytes("CODE"); //code.text
    protected static final byte[] Q_PARTOF = Bytes.toBytes("PART"); //partOf.reference
    protected static final byte[] Q_BASEDON = Bytes.toBytes("BASED"); //basedOn.reference
    protected static final byte[] Q_OWNER = Bytes.toBytes("OWNER"); //owner.reference
    protected static final byte[] Q_FOCUS = Bytes.toBytes("FOCUS"); //focus.reference
    
    protected static final byte[] Q_BODY = Bytes.toBytes("BODY");

    @Inject
    private HBaseConnector connector;

    FhirContext ctx = FhirContext.forR4();

    // TODO Note this will eventually have an enum for when the server is down
    protected Connection getConnection() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        return connector.getConnection();
    }

//    protected abstract StoreTaskOutcomeEnum saveToDatabase(IDomainResource resouce) throws Exception;

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

    protected Put processToPut(Task resource) {
        Put row = new Put(Bytes.toBytes(resource.getIdElement().getId()));

        addCode(resource, row);
        addLocation(resource, row);
        addStatus(resource, row);
        addPartOf(resource, row);
        addBasedOn(resource, row);
        addOwner(resource, row);
        addFocus(resource, row);
        

        row.addColumn(CF2, Q_BODY, Bytes.toBytes(parseResourceToJsonString(resource)));
        return row;
    }

    private void addCode(Task resource, Put row) {
        if (resource.getCode() != null && resource.getCode().getText() != null) {

            row.addColumn(CF1, Q_CODE, Bytes.toBytes(resource.getCode().getText()));
            LOG.debug("Entity type added: " + resource.getCode().getText());
        }
    }

    private void addLocation(Task resource, Put row) {
        if (resource.getLocation() != null &&resource.getLocation().getReference() != null) {
            row.addColumn(CF1, Q_LOCATION, Bytes.toBytes(resource.getLocation().getReference()));
            LOG.debug("Location type added: " + resource.getLocation().getReference());
        }
    }

    private void addStatus(Task resource, Put row) {
        if (resource.getStatus() != null && resource.getStatus().getDefinition() != null) {
            row.addColumn(CF1, Q_STATUS, Bytes.toBytes(resource.getStatus().getDefinition()));
            LOG.info("Status added: " + resource.getStatus().getDefinition());
        }
    }
    
    private void addPartOf(Task resource, Put row) {
        String partOf = resource.getPartOfFirstRep().getReference();
        if(StringUtils.isNotBlank(partOf)) {
            row.addColumn(CF1, Q_PARTOF, Bytes.toBytes(partOf));
            LOG.info("PartOf added: " + partOf);
        }
    }

    private void addBasedOn(Task resource, Put row) {
        String basedOn = resource.getBasedOnFirstRep().getReference();
        if(StringUtils.isNotBlank(basedOn)) {
            row.addColumn(CF1, Q_PARTOF, Bytes.toBytes(basedOn));
            LOG.info("BasedOn added: " + basedOn);
        }
    }
    
    private void addOwner(Task resource, Put row) {
        if (resource.getOwner() != null && resource.getOwner().getReference() != null) {
            row.addColumn(CF1, Q_OWNER, Bytes.toBytes(resource.getOwner().getReference()));
            LOG.info("Owner added: " + resource.getOwner().getReference());
        }
        
    }
    private void addFocus(Task resource, Put row) {
        if (resource.getFocus() != null && resource.getFocus().getReference() != null) {
            row.addColumn(CF1, Q_OWNER, Bytes.toBytes(resource.getFocus().getReference()));
            LOG.info("Focus added: " + resource.getFocus().getReference());
        }
    }

    protected void save(Put row) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (!getConnection().getAdmin().tableExists(TABLE_NAME)) {
            createTable();
        }
        Table table = getConnection().getTable(TABLE_NAME);
        table.put(row);
        save(row);
        LOG.debug("Save successful. Id: " + Bytes.toString(row.getRow()));
        table.close();
    }

    protected void save(List<Put> rows) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (!getConnection().getAdmin().tableExists(TABLE_NAME)) {
            createTable();
        }
        Table table = getConnection().getTable(TABLE_NAME);
        table.put(rows);
        save(rows);
        LOG.debug("Save successful.");
        table.close();
    }

    protected void createTable() throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
        Collection<ColumnFamilyDescriptor> families = new ArrayList<ColumnFamilyDescriptor>();
        families.add(ColumnFamilyDescriptorBuilder.of(CF1));
        families.add(ColumnFamilyDescriptorBuilder.of(CF2));
        builder.setColumnFamilies(families);
        TableDescriptor desc = builder.build();
        getConnection().getAdmin().createTable(desc);
    }

    @Override
    public Class<? extends IBaseResource> getResourceType() {
        return Task.class;
    }
}
