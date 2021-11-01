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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.audit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Put;
import org.hl7.fhir.r4.model.AuditEvent;

import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import net.fhirfactory.pegacorn.components.transaction.model.TransactionMethodOutcome;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.audit.common.AuditBaseProxy;

public class AuditBulkProxy extends AuditBaseProxy {

    @Create
    public MethodOutcome createEvent(@ResourceParam List<AuditEvent> theEvents) {
        return saveToDatabase(theEvents);
    }

    protected MethodOutcome saveToDatabase(List<AuditEvent> events) {
        TransactionMethodOutcome outcome = new TransactionMethodOutcome();
        try {
            List<Put> rows = new ArrayList<Put>();
            for (AuditEvent event : events) {
                Put row = processToPut(event);
                rows.add(row);
            }
            save(rows);
        } catch (MasterNotRunningException e) {
            populateBadOutcome(outcome, e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            populateBadOutcome(outcome, e.getMessage());
        } catch (IOException e) {
            populateBadOutcome(outcome, e.getMessage());
        }
        outcome.setCreated(false);
        return outcome;
    }

}
