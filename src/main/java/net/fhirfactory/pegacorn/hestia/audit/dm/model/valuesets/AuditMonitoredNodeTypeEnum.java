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
package net.fhirfactory.pegacorn.hestia.audit.dm.model.valuesets;

import net.fhirfactory.pegacorn.common.model.componentid.TopologyNodeTypeEnum;

public enum AuditMonitoredNodeTypeEnum {
    AUDIT_MONITORED_SUBSYSTEM("audit.monitored_node_type.subsystem"),
    AUDIT_MONITORED_SERVICE("audit.monitored_node_type.service"),
    AUDIT_MONITORED_PROCESSING_PLANT("audit.monitored_node_type.processingplant"),
    AUDIT_MONITORED_WORKSHOP("audit.monitored_node_type.workshop"),
    AUDIT_MONITORED_WORK_UNIT_PROCESSOR("audit.monitored_node_type.wup"),
    AUDIT_MONITORED_WORK_UNIT_PROCESSOR_COMPONENT("audit.monitored_node_type.wup_component"),
    AUDIT_MONITORED_ENDPOINT("audit.monitored_node_type.subsystem");

    private String nodeType;

    private AuditMonitoredNodeTypeEnum(String nodeType){
        this.nodeType = nodeType;
    }

    public String getNodeType() {
        return nodeType;
    }

    public static AuditMonitoredNodeTypeEnum nodeTypeFromTopologyNodeType(TopologyNodeTypeEnum nodeType){
        switch(nodeType){
            case WORKSHOP:
            case OAM_WORKSHOP:{
                return(AUDIT_MONITORED_WORKSHOP);
            }
            case OAM_WORK_UNIT_PROCESSOR:
            case WUP:{
                return(AUDIT_MONITORED_WORK_UNIT_PROCESSOR);
            }
            case ENDPOINT:{
                return(AUDIT_MONITORED_ENDPOINT);
            }
            case CLUSTER_SERVICE:{
                return(AUDIT_MONITORED_SERVICE);
            }
            case SUBSYSTEM:{
                return(AUDIT_MONITORED_SUBSYSTEM);
            }
            case PROCESSING_PLANT:{
                return(AUDIT_MONITORED_PROCESSING_PLANT);
            }
            case WUP_CORE:
            case WUP_INTERCHANGE_ROUTER:
            case WUP_CONTAINER_EGRESS_CONDUIT:
            case WUP_CONTAINER_INGRES_CONDUIT:
            case WUP_CONTAINER_EGRESS_PROCESSOR:
            case WUP_CONTAINER_INGRES_PROCESSOR:
            case WUP_CONTAINER_EGRESS_GATEKEEPER:
            case WUP_CONTAINER_INGRES_GATEKEEPER:
            case WUP_INTERCHANGE_PAYLOAD_TRANSFORMER:{
                return(AUDIT_MONITORED_WORK_UNIT_PROCESSOR_COMPONENT);
            }
            default:{
                return(null);
            }
        }
    }
}
