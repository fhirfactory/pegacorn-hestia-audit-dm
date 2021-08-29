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
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
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
    protected static final byte[] Q_NAME = Bytes.toBytes("NAME");
    protected static final byte[] Q_UPDATE = Bytes.toBytes("DATE");
    protected static final byte[] Q_PSTART = Bytes.toBytes("START");
    protected static final byte[] Q_PEND = Bytes.toBytes("END");
    protected static final byte[] Q_TYPE = Bytes.toBytes("TYPE");
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
        addSource(resource, row);
        addPurposeOfEvent(resource, row);
        row.addColumn(CF2, Q_BODY, Bytes.toBytes(parseResourceToJsonString(resource)));
        return row;
    }

    private void addAgent(AuditEvent resource, Put row) {
        if (resource.getAgent() != null) {
            for (AuditEventAgentComponent agent : resource.getAgent()) {
                // Store the first name found
                if (StringUtils.isNotBlank(agent.getName())) {
                    row.addColumn(CF1, Q_NAME, Bytes.toBytes(agent.getName()));
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
                row.addColumn(CF1, Q_PSTART, Bytes.toBytes(resource.getPeriod().getStart().getTime()));
                LOG.debug("Pending start added: " + resource.getPeriod().getStart().toString());

            }
            if (resource.getPeriod().getEnd() != null) {
                row.addColumn(CF1, Q_PEND, Bytes.toBytes(resource.getPeriod().getEnd().getTime()));
                LOG.debug("Pending end added: " + resource.getPeriod().getEnd().toString());
            }
        }
    }

    private void addSource(AuditEvent resource, Put row) {
        if (resource.getSource() != null) {
            StringBuilder sb = new StringBuilder();
            // TODO is this correct?
            for (Coding type : resource.getSource().getType()) {
                sb.append(type.getCode());
                sb.append(',');
            }
            if (sb.length() > 0) {
                row.addColumn(CF1, Q_TYPE, Bytes.toBytes(sb.substring(0, sb.length() - 1)));
                LOG.debug("Source added: " + sb.substring(0, sb.length() - 1));
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
