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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.devicemetric;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.DeviceMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import net.fhirfactory.pegacorn.components.transaction.model.TransactionMethodOutcome;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.devicemetric.common.DeviceMetricBaseProxy;

@ApplicationScoped
public class DeviceMetricProxy extends DeviceMetricBaseProxy {
    private static final Logger LOG = LoggerFactory.getLogger(DeviceMetricProxy.class);

    /**
     * Constructor
     */
    public DeviceMetricProxy() {
        initialiseTableName();
    }

    @Read()
    public DeviceMetric read(@IdParam IdType theId) {
        try {
            Connection connection = getConnection();
            Table table = connection.getTable(tableName);
            Get g = new Get(Bytes.toBytes(theId.getIdPart()));
            Result result = table.get(g);
            if (result.isEmpty()) {
                throw new ResourceNotFoundException(theId);
            }
            LOG.debug("Result not empty. Size: " + result.size());

            byte[] data = result.getValue(CF2, Q_BODY);
            String json = Bytes.toString(data);
            DeviceMetric task= (DeviceMetric) parseResourceFromJsonString(json);

            return task;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new ResourceNotFoundException(theId);
        }

    }

    @Create
    public MethodOutcome createDeviceMetric(@ResourceParam DeviceMetric theDeviceMetric) {
        LOG.debug(".createEvent(): Entry, theDeviceMetric (DeviceMetric) --> {}", theDeviceMetric);
        try {
            return saveToDatabase(theDeviceMetric);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new TransactionMethodOutcome();

    }

    @Update
    public MethodOutcome updateDeviceMetric(@ResourceParam DeviceMetric theDeviceMetric) {
        LOG.debug(".updateEvent(): Entry, theDeviceMetric (DeviceMetric) --> {}", theDeviceMetric);
        try {
            return saveToDatabase(theDeviceMetric);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return new TransactionMethodOutcome();
    }

    @Delete()
    public MethodOutcome deleteDeviceMetric(@IdParam IdType resourceId) {
        LOG.debug(".deleteDeviceMetric(): Entry, resourceId (IdType) --> {}", resourceId);
        throw (new UnsupportedOperationException("deleteDeviceMetric() is not supported"));
    }
    

    protected MethodOutcome saveToDatabase(DeviceMetric deviceMetric) {
        TransactionMethodOutcome outcome = new TransactionMethodOutcome();
        //TODO make outcome reflective of what happens in the transaction
        try {
            Put row = processToPut(deviceMetric);
            save(row);
            outcome.setId(deviceMetric.getIdElement());
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outcome;
    }
}
