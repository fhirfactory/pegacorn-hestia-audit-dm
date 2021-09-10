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
package net.fhirfactory.pegacorn.hestia.audit.dm;

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
import org.hl7.fhir.r4.model.AuditEvent;
import org.hl7.fhir.r4.model.AuditEvent.AuditEventAgentComponent;
import org.hl7.fhir.r4.model.AuditEvent.AuditEventEntityComponent;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.IResourceProvider;

public abstract class AuditBaseProxy implements IResourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AuditBaseProxy.class);

    protected static final TableName TABLE_NAME = TableName.valueOf("AUDIT_EVENT");
    protected static final byte[] CF1 = Bytes.toBytes("INFO");
    protected static final byte[] CF2 = Bytes.toBytes("DATA");
    protected static final byte[] Q_AGENT_NAME = Bytes.toBytes("NAME");
    protected static final byte[] Q_UPDATE = Bytes.toBytes("DATE");
    protected static final byte[] Q_PERIOD_START = Bytes.toBytes("START");
    protected static final byte[] Q_PERIOD_END = Bytes.toBytes("END");
    protected static final byte[] Q_SOURCE = Bytes.toBytes("SOURCE"); // Source site
    protected static final byte[] Q_ENTITY_TYPE = Bytes.toBytes("TYPE"); // Entity code
    protected static final byte[] Q_ENTITY_NAME = Bytes.toBytes("ENTITY"); // Entity name
    protected static final byte[] Q_PURPOSE = Bytes.toBytes("PURPOSE");
    protected static final byte[] Q_BODY = Bytes.toBytes("BODY");

    @Inject
    private HBaseConnector connector;
    FhirContext ctx = FhirContext.forR4();

    // TODO Note this will eventually have an enum for when the server is down
    protected Connection getConnection() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        return connector.getConnection();
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

    protected Put processToPut(AuditEvent resource) {
        Put row = new Put(Bytes.toBytes(resource.getIdElement().getId()));

        addAgent(resource, row);
        addUpdateDate(resource, row);
        addPeriod(resource, row);
        addSourceSite(resource, row);
        addEntityDetails(resource, row);
        addPurposeOfEvent(resource, row);
        row.addColumn(CF2, Q_BODY, Bytes.toBytes(parseResourceToJsonString(resource)));
        return row;
    }

    private void addAgent(AuditEvent resource, Put row) {
        if (resource.getAgent() != null) {
            for (AuditEventAgentComponent agent : resource.getAgent()) {
                // Store the first name found
                if (StringUtils.isNotBlank(agent.getName())) {
                    row.addColumn(CF1, Q_AGENT_NAME, Bytes.toBytes(agent.getName()));
                    LOG.debug("Agent added: " + agent.getName());
                    break;
                }
            }
        }
    }

    private void addUpdateDate(AuditEvent resource, Put row) {
        if (resource.getMeta() != null && resource.getMeta().getLastUpdated() != null) {
            row.addColumn(CF1, Q_UPDATE, Bytes.toBytes(resource.getMeta().getLastUpdated().getTime()));
            LOG.debug("Update date added: " + resource.getMeta().getLastUpdated().toString());
        }
    }

    private void addPeriod(AuditEvent resource, Put row) {
        if (resource.getPeriod() != null) {
            if (resource.getPeriod().getStart() != null) {
                row.addColumn(CF1, Q_PERIOD_START, Bytes.toBytes(resource.getPeriod().getStart().getTime()));
                LOG.debug("Pending start added: " + resource.getPeriod().getStart().toString());

            }
            if (resource.getPeriod().getEnd() != null) {
                row.addColumn(CF1, Q_PERIOD_END, Bytes.toBytes(resource.getPeriod().getEnd().getTime()));
                LOG.debug("Pending end added: " + resource.getPeriod().getEnd().toString());
            }
        }
    }

    private void addSourceSite(AuditEvent resource, Put row) {
        if (resource.getSource() != null) {

            row.addColumn(CF1, Q_SOURCE, Bytes.toBytes(resource.getSource().getSite()));
            LOG.debug("Entity type added: " + resource.getSource().getSite());

        }
    }

    private void addEntityDetails(AuditEvent resource, Put row) {
        if (resource.getEntity() != null) {
            StringBuilder code = new StringBuilder();
            StringBuilder name = new StringBuilder();
            for (AuditEventEntityComponent entity : resource.getEntity()) {
                if (entity.getType() != null && entity.getType().getCode() != null) {
                    code.append(entity.getType().getCode());
                    code.append(',');
                }
                if(StringUtils.isNotBlank(entity.getName())) {
                    name.append(entity.getName());
                    name.append(',');
                }
            }
            if (name.length() > 0) {
                row.addColumn(CF1, Q_ENTITY_NAME, Bytes.toBytes(name.substring(0, name.length() - 1)));
                LOG.debug("Entity type added: " + name.substring(0, name.length() - 1));
            }
            if (code.length() > 0) {
                row.addColumn(CF1, Q_ENTITY_TYPE, Bytes.toBytes(code.substring(0, code.length() - 1)));
                LOG.debug("Entity type added: " + code.substring(0, code.length() - 1));
            }
        }
    }

    private void addPurposeOfEvent(AuditEvent resource, Put row) {
        if (resource.getPurposeOfEvent() != null) {
            StringBuilder sb = new StringBuilder();
            for (CodeableConcept purpose : resource.getPurposeOfEvent()) {
                sb.append(purpose.getTextElement());
                sb.append(',');
            }
            if (sb.length() > 0) {
                row.addColumn(CF1, Q_PURPOSE, Bytes.toBytes(sb.substring(0, sb.length() - 1)));
                LOG.debug("Purpose added: " + sb.substring(0, sb.length() - 1));
            }
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
        return AuditEvent.class;
    }
}
