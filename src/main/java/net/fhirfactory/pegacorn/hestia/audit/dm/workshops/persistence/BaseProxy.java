package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
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
   
    @Inject
    protected HBaseConnector connector;
    
    FhirContext ctx = FhirContext.forR4();

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
    

    protected void save(Put row) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (!getConnection().getAdmin().tableExists(getTableName())) {
            createTable();
        }
        Table table = getConnection().getTable(getTableName());
        table.put(row);
        LOG.debug("Save successful. Id: " + Bytes.toString(row.getRow()));
        table.close();
    }

    protected void save(List<Put> rows) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (!getConnection().getAdmin().tableExists(getTableName())) {
            createTable();
        }
        Table table = getConnection().getTable(getTableName());
        table.put(rows);
        LOG.debug("Save successful.");
        table.close();
    }
    protected void createTable() throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(getTableName());
        Collection<ColumnFamilyDescriptor> families = getColumnFamilies();
        builder.setColumnFamilies(families);
        TableDescriptor desc = builder.build();
        getConnection().getAdmin().createTable(desc);
    }
    
    //
    // Search common methods
    //
    
    /*
     * Needed because some of the names can have special characters that would be
     * compiled by the regex comparator Currently only handling (). but can be
     * expanded later
     */
    protected String prepareRegex(String string) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            switch (string.charAt(i)) {
            case '(':
            case ')':
            case '.':
                sb.append("\\");
            }
            sb.append(string.charAt(i));
        }
        return sb.substring(0);
    }

    protected Date parseDateString(String dateString) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        return sdf.parse(dateString);
    }

    protected Date parseEndRange(String dateString) throws ParseException {
        Calendar endRange = Calendar.getInstance();
        endRange.setTime(parseDateString(dateString));
        LOG.info("Parsed time: " + endRange.toString());
        endRange.add(Calendar.MINUTE, 1);
        return endRange.getTime();
    }
    
    protected List<String> getResults(FilterList filterList) {
        return getResults(filterList, -1, false);
    } 
    
    protected List<String> getResults(FilterList filterList, int limit, boolean reverse) {

        List<String> events = new ArrayList<String>();

        try {
            Table table = getConnection().getTable(getTableName());
            Scan scan = new Scan().setFilter(filterList);
            scan.setReversed(reverse);
            scan.setLimit(limit);

            ResultScanner results = table.getScanner(scan);

            if (results != null) {
                Result result;
                result = results.next();

                while (result != null) {
                    LOG.debug("rowkey=" + Bytes.toString(result.getRow()));

                    String data = Bytes.toString(extractJSONFromResult(result));
                    if (data != null) {
                        events.add(data);
                    }
                    result = results.next();
                }
            }
            results.close();
            return events;
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return events;
    }

    
    /*
     * TableName needs to be set in each subclass so it can be used by create / save
     */
    protected abstract TableName getTableName();
    /*
     * column families need to be specified upon table creation. 
     */
    protected abstract Collection<ColumnFamilyDescriptor> getColumnFamilies();
    
    /*
     * Method to set the right column group / qualifier for extraction
     */
    protected abstract byte[] extractJSONFromResult(Result result);
            

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
