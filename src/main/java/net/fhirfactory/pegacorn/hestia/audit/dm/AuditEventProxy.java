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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.NameValuePair;
import org.hl7.fhir.r4.model.AuditEvent;
import org.hl7.fhir.r4.model.IdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;

@ApplicationScoped
public class AuditEventProxy extends AuditBaseProxy {
    private static final Logger LOG = LoggerFactory.getLogger(AuditEventProxy.class);

    /**
     * Constructor
     */
    public AuditEventProxy() {

    }

    @Read()
    public AuditEvent read(@IdParam IdType theId) {
        try {
            Connection connection = getConnection();
            Table table = connection.getTable(TABLE_NAME);
            Get g = new Get(Bytes.toBytes(theId.getIdPart()));
            Result result = table.get(g);
            if (result.isEmpty()) {
                throw new ResourceNotFoundException(theId);
            }
            LOG.debug("Result not empty. Size: " + result.size());

            byte[] data = result.getValue(CF2, Q_BODY);
            String json = Bytes.toString(data);
            AuditEvent audit = (AuditEvent) parseResourceFromJsonString(json);

            return audit;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new ResourceNotFoundException(theId);
        }

    }

    @Create
    public StoreAuditOutcomeEnum createEvent(@ResourceParam AuditEvent theEvent) {
        LOG.debug(".createEvent(): Entry, theEvent (AuditEvent) --> {}", theEvent);
        try {
            return saveToDatabase(theEvent);
        } catch (Exception e) {
            e.printStackTrace();
            return StoreAuditOutcomeEnum.BAD;
        }
    }

    @Update
    public StoreAuditOutcomeEnum updateEvent(@ResourceParam AuditEvent theEvent) {
        LOG.debug(".updateEvent(): Entry, theEvent (AuditEvent) --> {}", theEvent);
        try {
            return saveToDatabase(theEvent);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return StoreAuditOutcomeEnum.BAD;

    }

    @Delete()
    public StoreAuditOutcomeEnum deleteEvent(@IdParam IdType resourceId) {
        LOG.debug(".deleteEvent(): Entry, resourceId (IdType) --> {}", resourceId);
        throw (new UnsupportedOperationException("deleteEvent() is not supported"));
    }

    public List<String> getByUser(@ResourceParam String agentName) {
        Filter f = new DependentColumnFilter(CF1, Q_AGENT_NAME, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(agentName)));
        FilterList filterList = new FilterList(f);
        return getResults(filterList);
    }

    public List<String> getByTypeAndDate(@ResourceParam String entityType, @ResourceParam String dateString) throws Throwable {
        LOG.debug("Searching for Entity: " + entityType + " and Date: " + dateString);
        Date startRange = parseDateString(dateString);
        Date endRange = parseEndRange(dateString);
        Filter typeFilter = new DependentColumnFilter(CF1, Q_ENTITY_TYPE, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(entityType) + "$"));
        // StartDate needs to be less than end of range
        Filter startFilter = new DependentColumnFilter(CF1, Q_PERIOD_START, true, CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(endRange.getTime())));
        // EndDate needs to be greater than start of range
        Filter endFilter = new DependentColumnFilter(CF1, Q_PERIOD_END, true, CompareOperator.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(startRange.getTime())));

        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
        filterList.addFilter(endFilter);
        filterList.addFilter(startFilter);
        filterList.addFilter(typeFilter);

        return getResults(filterList);
    }
    
    //source site / period / entity name
    public List<String> getBySiteNameAndDate(@ResourceParam String site, @ResourceParam String entityName, @ResourceParam String dateString) throws Throwable {
        LOG.debug("Searching for : " + site + ", Entity Name: " + entityName + " and Date: " + dateString);
        Date startRange = parseDateString(dateString);
        Date endRange = parseEndRange(dateString);
        
        Filter siteFilter = new DependentColumnFilter(CF1, Q_SOURCE, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(site) + "$"));
        Filter nameFilter = new DependentColumnFilter(CF1, Q_ENTITY_NAME, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(entityName) + "$"));
        // StartDate needs to be less than end of range
        Filter startFilter = new DependentColumnFilter(CF1, Q_PERIOD_START, true, CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(endRange.getTime())));
        // EndDate needs to be greater than start of range
        Filter endFilter = new DependentColumnFilter(CF1, Q_PERIOD_END, true, CompareOperator.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(startRange.getTime())));

        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

        filterList.addFilter(siteFilter);
        List<String> list = getResults(filterList);
        LOG.info("Site filter returned result:" + list.size());
        filterList.addFilter(nameFilter);
        list = getResults(filterList);
        LOG.info("Name and site filter returned result:" + list.size());
        filterList.addFilter(endFilter);
        filterList.addFilter(startFilter);

        return getResults(filterList);
    }
   

    private static String prepareRegex(String string) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < string.length(); i++) {
            switch(string.charAt(i)) {
            case '(':
            case ')':
            case '.':
                sb.append("\\");
            }
            sb.append(string.charAt(i));
        }
        return sb.substring(0);
    }

    protected StoreAuditOutcomeEnum saveToDatabase(AuditEvent event) {
        try {
            Put row = processToPut(event);
            save(row);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            return StoreAuditOutcomeEnum.FAILED;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            return StoreAuditOutcomeEnum.FAILED;
        } catch (IOException e) {
            e.printStackTrace();
            return StoreAuditOutcomeEnum.BAD;
        }
        return StoreAuditOutcomeEnum.GOOD;
    }

    Date parseDateString(String dateString) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        return sdf.parse(dateString);
    }

    Date parseEndRange(String dateString) throws ParseException {
        Calendar endRange = Calendar.getInstance();
        endRange.setTime(parseDateString(dateString));
        LOG.info("Parsed time: " + endRange.toString());
        endRange.add(Calendar.MINUTE, 1);
        return endRange.getTime();
    }

    protected List<String> getResults(FilterList filterList) {
        List<String> events = new ArrayList<String>();

        try {
            Table table = getConnection().getTable(TABLE_NAME);
            Scan scan = new Scan().setFilter(filterList);

            ResultScanner results = table.getScanner(scan);

            if (results != null) {
                Result result;
                result = results.next();

                while (result != null) {
                    LOG.debug("rowkey=" + Bytes.toString(result.getRow()));

                    String data = Bytes.toString(result.getValue(CF2, Q_BODY));
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
        return null;
    }

}
