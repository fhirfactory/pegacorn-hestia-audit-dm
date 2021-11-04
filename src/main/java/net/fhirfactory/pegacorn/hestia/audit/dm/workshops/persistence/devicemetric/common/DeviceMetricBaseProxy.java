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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.devicemetric.common;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.DeviceMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.rest.server.IResourceProvider;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.BaseProxy;

public abstract class DeviceMetricBaseProxy extends BaseProxy implements IResourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DeviceMetricBaseProxy.class);

    protected static final TableName TABLE_NAME = TableName.valueOf("DEVICE_METRIC");
    protected static final byte[] CF1 = Bytes.toBytes("INFO");
    protected static final byte[] CF2 = Bytes.toBytes("DATA");
    
    protected static final byte[] Q_TYPE = Bytes.toBytes("TYPE"); 
    protected static final byte[] Q_UNIT = Bytes.toBytes("UNIT"); 
    protected static final byte[] Q_SOURCE = Bytes.toBytes("SOURCE"); 
    protected static final byte[] Q_PARENT = Bytes.toBytes("PARENT"); 
    
    
    protected static final byte[] Q_BODY = Bytes.toBytes("BODY");
    
    @Override
    protected TableName getTableName() {
        return TABLE_NAME;
    }

    protected Put processToPut(DeviceMetric resource) {
        Put row = new Put(Bytes.toBytes(resource.getIdElement().getId()));
        LOG.info("Resource: " + resource.toString());

        addType(resource, row);
        addUnit(resource, row);
        addSource(resource, row);
        addParent(resource, row);
        
        row.addColumn(CF2, Q_BODY, Bytes.toBytes(parseResourceToJsonString(resource)));
        return row;
    }

    private void addType(DeviceMetric resource, Put row) {
        LOG.info("getType() is " + resource.getType());     
        if (resource.hasType() && resource.getType().getTextElement() != null) {

            row.addColumn(CF1, Q_TYPE, Bytes.toBytes(resource.getType().getTextElement().asStringValue()));
            LOG.info("Type added: " + resource.getType().getTextElement());
        }
    }

    private void addUnit(DeviceMetric resource, Put row) {
        LOG.info("getUnit() is " + resource.getUnit());     
        if (resource.getUnit() != null &&resource.getUnit().getTextElement() != null) {
            row.addColumn(CF1, Q_UNIT, Bytes.toBytes(resource.getUnit().getTextElement().asStringValue()));
            LOG.debug("Unit added: " + resource.getUnit().getTextElement().asStringValue());
        }
    }

    private void addSource(DeviceMetric resource, Put row) {
        LOG.info("getSource() is " + resource.getSource());     
        if (resource.getSource() != null && resource.getSource().getIdBase() != null) {
            row.addColumn(CF1, Q_SOURCE, Bytes.toBytes(resource.getSource().getIdBase()));
            LOG.info("Source added: " + resource.getSource().getIdBase());
        }
    }
    
    private void addParent(DeviceMetric resource, Put row) {
        LOG.info("getParent() is " + resource.getParent());     
        if (resource.getParent() != null && resource.getParent().getIdBase() != null) {
            row.addColumn(CF1, Q_PARENT, Bytes.toBytes(resource.getParent().getIdBase()));
            LOG.info("Parent added: " + resource.getParent().getIdBase());
        }
    }

    
    @Override
    protected Collection<ColumnFamilyDescriptor> getColumnFamilies() {
        Collection<ColumnFamilyDescriptor> families = new ArrayList<ColumnFamilyDescriptor>();
        families.add(ColumnFamilyDescriptorBuilder.of(CF1));
        families.add(ColumnFamilyDescriptorBuilder.of(CF2));
        return families;
    }
    
    
    @Override
    protected byte[] extractJSONFromResult(Result result) {
        return result.getValue(CF2, Q_BODY);
    }

    @Override
    public Class<? extends IBaseResource> getResourceType() {
        return DeviceMetric.class;
    }
}
