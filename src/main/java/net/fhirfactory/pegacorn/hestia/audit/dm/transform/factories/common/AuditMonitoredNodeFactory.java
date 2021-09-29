/*
 * Copyright (c) 2021 Mark A. Hunter (ACT Health)
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
package net.fhirfactory.pegacorn.hestia.audit.dm.transform.factories.common;

import org.slf4j.Logger;

import net.fhirfactory.pegacorn.deployment.topology.model.common.TopologyNode;
import net.fhirfactory.pegacorn.hestia.audit.dm.model.common.AuditMonitoredNode;
import net.fhirfactory.pegacorn.hestia.audit.dm.model.valuesets.AuditMonitoredNodeTypeEnum;

public abstract class AuditMonitoredNodeFactory {

    abstract protected Logger getLogger();

    protected AuditMonitoredNode newAuditMonitoredNode(AuditMonitoredNode monitoredNode, TopologyNode topologyNode){
        monitoredNode.setNodeID(topologyNode.getComponentID());
        monitoredNode.setComponentName(topologyNode.getNodeRDN().getNodeName());
        AuditMonitoredNodeTypeEnum nodeTypeEnum = AuditMonitoredNodeTypeEnum.nodeTypeFromTopologyNodeType(topologyNode.getComponentType());
        monitoredNode.setNodeType(nodeTypeEnum);
        monitoredNode.setNodeVersion(topologyNode.getNodeRDN().getNodeVersion());
        if(topologyNode.getConcurrencyMode() != null) {
            monitoredNode.setConcurrencyMode(topologyNode.getConcurrencyMode().getDisplayName());
        }
        if(topologyNode.getResilienceMode() != null) {
            monitoredNode.setResilienceMode(topologyNode.getResilienceMode().getDisplayName());
        }
        return(monitoredNode);
    }
}
