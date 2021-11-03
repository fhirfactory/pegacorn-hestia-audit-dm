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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.capability;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.IdType;
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
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.capability.common.CapabilityStatementBaseProxy;

@ApplicationScoped
public class CapabilityStatementProxy extends CapabilityStatementBaseProxy {
    private static final Logger LOG = LoggerFactory.getLogger(CapabilityStatementProxy.class);

    /**
     * Constructor
     */
    public CapabilityStatementProxy() {
    }

    @Read()
    public CapabilityStatement read(@IdParam IdType theId) {
        try {
            Connection connection = getConnection();
            Table table = connection.getTable(getTableName());
            Get g = new Get(Bytes.toBytes(theId.getIdPart()));
            Result result = table.get(g);
            if (result.isEmpty()) {
                throw new ResourceNotFoundException(theId);
            }
            LOG.debug("Result not empty. Size: " + result.size());

            byte[] data = result.getValue(CF2, Q_BODY);
            String json = Bytes.toString(data);
            CapabilityStatement task= (CapabilityStatement) parseResourceFromJsonString(json);

            return task;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new ResourceNotFoundException(theId);
        }

    }

    @Create
    public MethodOutcome createCapabilityStatement(@ResourceParam CapabilityStatement theCapabilityStatement) {
        LOG.debug(".createCapabilityStatement(): Entry, theCapabilityStatement (CapabilityStatement) --> {}", theCapabilityStatement);
        try {
            return saveToDatabase(theCapabilityStatement);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new TransactionMethodOutcome();

    }

    @Update
    public MethodOutcome updateCapabilityStatement(@ResourceParam CapabilityStatement theStatement) {
        LOG.debug(".updateCapabilityStatement(): Entry, theCapabilityStatement (CapabilityStatement) --> {}", theStatement);
        try {
            return saveToDatabase(theStatement);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return new TransactionMethodOutcome();
    }

    @Delete()
    public MethodOutcome deleteCapabilityStatement(@IdParam IdType resourceId) {
        LOG.debug(".deleteCapabilityStatement(): Entry, resourceId (IdType) --> {}", resourceId);
        throw (new UnsupportedOperationException("deleteCapabilityStatement() is not supported"));
    }
    

    protected MethodOutcome saveToDatabase(CapabilityStatement device) {
        TransactionMethodOutcome outcome = new TransactionMethodOutcome();
        //TODO make outcome reflective of what happens in the transaction
        try {
            Put row = processToPut(device);
            save(row);
            outcome.setId(device.getIdElement());
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