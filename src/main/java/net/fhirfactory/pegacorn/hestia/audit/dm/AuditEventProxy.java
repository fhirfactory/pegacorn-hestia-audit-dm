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
        Filter f = new DependentColumnFilter(CF1, Q_NAME, true, CompareOperator.EQUAL, new RegexStringComparator("^" + agentName));
        FilterList filterList = new FilterList(f);
        return getResults(filterList);
    }
    
    public List<String> getByTypeAndDate(@ResourceParam String entityType, @ResourceParam String dateString) throws Throwable {
        LOG.info("Searching for Entity: " + entityType + " and Date: " + dateString);
        Date startRange = parseDateString(dateString);
        Date endRange = parseEndRange(dateString);
        LOG.info("For dateString: " + dateString + " - Range from " + startRange.toString() + " to " + endRange.toString());
        LOG.info("Start (long): " + startRange.getTime() + ". End (long): " + endRange.getTime());
        //TODO date granularity
        Filter typeFilter = new DependentColumnFilter(CF1, Q_TYPE, true, CompareOperator.EQUAL, new RegexStringComparator("^" + entityType + "$"));
        //StartDate needs to be greater than start of range
        Filter startFilter1 = new DependentColumnFilter(CF1, Q_PSTART, false, CompareOperator.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(startRange.getTime())));
        //StartDate needs to be less than end of range
        Filter startFilter2 = new DependentColumnFilter(CF1, Q_PSTART, false, CompareOperator.GREATER,
                new BinaryComparator(Bytes.toBytes(endRange.getTime())));
        Filter endFilter1 = new DependentColumnFilter(CF1, Q_PEND, false, CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(startRange.getTime())));
        Filter endFilter2 = new DependentColumnFilter(CF1, Q_PEND, false, CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(endRange.getTime())));

        FilterList startTimeChecker = new FilterList(Operator.MUST_PASS_ONE);
        startTimeChecker.addFilter(startFilter1);
        startTimeChecker.addFilter(startFilter2);
        
        FilterList endTimeChecker = new FilterList(Operator.MUST_PASS_ONE);
        endTimeChecker.addFilter(endFilter1);
        endTimeChecker.addFilter(endFilter2);
        
        //TYPE && (START1 || START2) && (END1 || END2) - should work in all logical cases
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
        filterList.addFilter(typeFilter);
        filterList.addFilter(startTimeChecker);
        filterList.addFilter(endTimeChecker);
        
        //Trying to see what's going on where multiple filters are being used
//        List<String> list = getResults(startTimeChecker);
//        LOG.info("startTimeChecker sa: " + list.size() + ". checker.size: " + startTimeChecker.size());
////        startTimeChecker.addFilter(startFilter2);
//        startTimeChecker.addFilter(startFilter2);
//        list = getResults(startTimeChecker);
//        LOG.info("startTimeChecker both: " + list.size() + ". checker.size: " + startTimeChecker.size());
//        list = getResults(endTimeChecker);
//        LOG.info("endTimeChecker be: " + list.size() + ". checker.size: " + endTimeChecker.size());
//        endTimeChecker.addFilter(endFilter1);
//        list = getResults(endTimeChecker);
//        LOG.info("endTimeChecker both: " + list.size() + ". checker.size: " + endTimeChecker.size());
//        
        
        
        return getResults(filterList);
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
