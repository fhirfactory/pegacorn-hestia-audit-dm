/*
 * Copyright (c) 2021 Kelly Skye (ACT Health)
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
package net.fhirfactory.pegacorn.hestia.audit.dm.transform.factories;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fhirfactory.pegacorn.common.model.componentid.TopologyNodeFDN;
import net.fhirfactory.pegacorn.deployment.topology.model.nodes.WorkUnitProcessorTopologyNode;
import net.fhirfactory.pegacorn.deployment.topology.model.nodes.WorkshopTopologyNode;
import net.fhirfactory.pegacorn.hestia.audit.dm.model.AuditMonitoredWUP;
import net.fhirfactory.pegacorn.hestia.audit.dm.model.AuditMonitoredWorkshop;
import net.fhirfactory.pegacorn.hestia.audit.dm.transform.factories.common.AuditMonitoredNodeFactory;
import net.fhirfactory.pegacorn.petasos.endpoints.oam.hestia.audit.AuditDiscoveredNodesDM;

@ApplicationScoped
public class AuditMonitoredWorkshopFactory  extends AuditMonitoredNodeFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AuditMonitoredWorkshopFactory.class);

    @Inject
    private AuditDiscoveredNodesDM nodeDM;

    @Inject
    private AuditMonitoredWUPFactory wupFactory;

    @Override
    protected Logger getLogger() {
        return (LOG);
    }

    public AuditMonitoredWorkshop newWorkshop(WorkshopTopologyNode workshopNode){
        AuditMonitoredWorkshop workshop = new AuditMonitoredWorkshop();
        workshop = (AuditMonitoredWorkshop) newAuditMonitoredNode(workshop, workshopNode);
        for(TopologyNodeFDN currentWUPFDN: workshopNode.getWupSet()){
            WorkUnitProcessorTopologyNode wupTopologyNode = (WorkUnitProcessorTopologyNode) nodeDM.getTopologyNode(currentWUPFDN);
            AuditMonitoredWUP currentWUP = wupFactory.newWorkUnitProcessor(wupTopologyNode);
            workshop.getWorkUnitProcessors().add(currentWUP);
        }
        return(workshop);
    }

}
