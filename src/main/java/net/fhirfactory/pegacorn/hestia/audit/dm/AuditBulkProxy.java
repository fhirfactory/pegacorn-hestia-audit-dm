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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Put;
import org.hl7.fhir.r4.model.AuditEvent;

import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.ResourceParam;

public class AuditBulkProxy extends AuditBaseProxy {

    @Create
    public StoreAuditOutcomeEnum createEvent(@ResourceParam List<AuditEvent> theEvents) {
        return saveToDatabase(theEvents);
    }

    // TODO KS make asynch and return starting
    protected StoreAuditOutcomeEnum saveToDatabase(List<AuditEvent> events) {
        try {
            List<Put> rows = new ArrayList<Put>();
            for (AuditEvent event : events) {
                Put row = processToPut(event);
                rows.add(row);
            }
            save(rows);
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

}
