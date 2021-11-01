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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.edge.answer;

import java.util.ArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.OnExceptionDefinition;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestParamType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fhirfactory.pegacorn.common.model.componentid.TopologyNodeFDN;
import net.fhirfactory.pegacorn.components.transaction.valuesets.exceptions.ResourceNotFoundException;
import net.fhirfactory.pegacorn.components.transaction.valuesets.exceptions.ResourceUpdateException;
import net.fhirfactory.pegacorn.deployment.topology.model.endpoints.base.IPCTopologyEndpoint;
import net.fhirfactory.pegacorn.deployment.topology.model.endpoints.technologies.HTTPServerClusterServiceTopologyEndpointPort;
import net.fhirfactory.pegacorn.hestia.audit.dm.common.HestiaAuditDMNames;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.audit.AuditSearchProxy;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.task.TaskSearchProxy;
import net.fhirfactory.pegacorn.internals.PegacornReferenceProperties;
import net.fhirfactory.pegacorn.petasos.core.moa.wup.MessageBasedWUPEndpoint;
import net.fhirfactory.pegacorn.workshops.InteractWorkshop;
import net.fhirfactory.pegacorn.workshops.base.Workshop;
import net.fhirfactory.pegacorn.wups.archetypes.unmanaged.NonResilientWithAuditTrailWUP;

@ApplicationScoped
public class AuditHTTPServer extends NonResilientWithAuditTrailWUP {

    private static final Logger LOG = LoggerFactory.getLogger(AuditHTTPServer.class);
    
    private static String WUP_VERSION = "1.0.0";

    @Inject
    private InteractWorkshop workshop;

    @Inject
    private AuditSearchProxy auditSearchProxy;
    
    @Inject
    private TaskSearchProxy taskSearchProxy;
    
    @Inject
    private HestiaAuditDMNames names;
    
    @Inject
    private PegacornReferenceProperties pegacornReferenceProperties;


    private String serverHostName;
    private int serverHostPort;
    private String hTTPScheme;
    
    //Candidate for deletion
//    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//        LOG.info("Get called");
//        List<NameValuePair> params = URLEncodedUtils.parse(request.getQueryString(), Charset.defaultCharset());
//
//        String entityType = null, date = null, agentName = null, entityName = null, site = null;
//        for (NameValuePair param : params) {
//            if("entity-type".equals(param.getName())) {
//                entityType = param.getValue();
//            }
//            if("date".equals(param.getName())) {
//                date = param.getValue();
//            }
//            if("agent-name".equals(param.getName())) {
//                agentName = param.getValue();
//            }
//            if("entity-name".equals(param.getName())) {
//                entityName = param.getValue();
//            }
//            if("site".equals(param.getName())) {
//                site = param.getValue();
//            }
//            LOG.info(param.getName() + " : " + param.getValue());
//        }
//
//        int responseStatusCode = HttpServletResponse.SC_OK;
//        String responseMsg = null;
//        try {
//            if(StringUtils.isNotBlank(agentName)) {
//                responseMsg = parseResults(auditSearchProxy.getByUser(agentName));
//            } else if (StringUtils.isNotBlank(entityType) && StringUtils.isNotBlank(date)) {
//                responseMsg = parseResults(auditSearchProxy.getByTypeAndDate(entityType, date));
//            } else if (StringUtils.isNotBlank(site) && StringUtils.isNotBlank(entityName) && StringUtils.isNotBlank(date)) {
//                responseMsg = parseResults(auditSearchProxy.getBySiteNameAndDate(entityName, date, site));
//            } else {
//                responseMsg = "Invalid Parameters";
//            }
//        } catch (Exception e) {
//            LOG.error("Exception occurred performing search", e);
//            responseStatusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
//        } catch (Throwable e) {
//            LOG.error("Date unable to be parsed: " + e.getMessage());
//            responseMsg = "Invalid date passed";
//        }
//        response.setStatus(responseStatusCode);
//        if (responseMsg != null) {
//            response.getWriter().write(responseMsg);
//            response.getWriter().flush();
//        }
//    }
//    
//    private String parseResults(List<String> values) {
//        if(values == null || values.size() == 0) {
//            return "No results found";
//        }
//        StringBuilder sb = new StringBuilder();
//        for(String value : values) {
//            sb.append(value);
//            sb.append(',');
//        }
//        return sb.substring(0, sb.length() -1);
//    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected String specifyWUPInstanceName() {
        return (getClass().getSimpleName());
    }

    @Override
    protected String specifyWUPInstanceVersion() {
        return (WUP_VERSION);
    }

    @Override
    protected Workshop specifyWorkshop() {
        return workshop;
    }

    @Override
    protected void executePostConstructActivities() {
        deriveEndpointDetails();
    }
    //
    // Getters (and Setters)
    //

    public PegacornReferenceProperties getPegacornReferenceProperties() {
        return pegacornReferenceProperties;
    }


    protected String getServerHostName(){
        return(this.serverHostName);
    }

    protected int getServerHostPort(){
        return(this.serverHostPort);
    }

    protected String getHTTPScheme(){
        return(this.hTTPScheme);
    }


    @Override
    public void configure() throws Exception {

        restConfiguration()
            .component("netty-http")
            .scheme(getHTTPScheme())
            .bindingMode(RestBindingMode.json)
            .dataFormatProperty("moduleClassNames", "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule")
            .dataFormatProperty("disableFeatures", "WRITE_DATES_AS_TIMESTAMPS")
            .dataFormatProperty("prettyPrint", "true")
            .contextPath(getPegacornReferenceProperties().getAuditDMContextPath())
            .host(getServerHostName())
            .port(getServerHostPort());

        rest("/AuditEvent")
             .get("?agent-name={agentName}&entity-type={entityType}&entity-name={entityName}&date={date}&site={site}&limit={limit}")
                .param().name("agentName").type(RestParamType.query).required(false).endParam()
                .param().name("entityType").type(RestParamType.query).required(false).endParam()
                .param().name("entityName").type(RestParamType.query).required(false).endParam()
                .param().name("date").type(RestParamType.query).required(false).endParam()
                .param().name("site").type(RestParamType.query).required(false).endParam()
                .param().name("limit").type(RestParamType.query).required(false).endParam()
                .to("direct:AuditEventGeneralSearch");
        
        from("direct:AuditEventGeneralSearch")
            .log(LoggingLevel.INFO, "General Search")
            .bean(auditSearchProxy, "doSearch");

        
        //partOf, basedOn, code, status, location, owner, focus
        rest("/Task")
             .get("?location={location}&code={code}&part-of={partOf}&based-on={basedOn}&status={status}&owner={owner}&focus={focus}&limit={limit}")
                .param().name("location").type(RestParamType.query).required(false).endParam()
                .param().name("code").type(RestParamType.query).required(false).endParam()
                .param().name("partOf").type(RestParamType.query).required(false).endParam()
                .param().name("basedOn").type(RestParamType.query).required(false).endParam()
                .param().name("status").type(RestParamType.query).required(false).endParam()
                .param().name("owner").type(RestParamType.query).required(false).endParam()
                .param().name("focus").type(RestParamType.query).required(false).endParam()
                .param().name("limit").type(RestParamType.query).required(false).endParam()
                .to("direct:TaskGeneralSearch");

        from("direct:TaskGeneralSearch")
            .log(LoggingLevel.INFO, "General Search")
            .bean(taskSearchProxy, "doSearch");
    }

    //
    // Endpoint Details Derivation
    //

    protected void deriveEndpointDetails() {
        MessageBasedWUPEndpoint endpoint = new MessageBasedWUPEndpoint();
        HTTPServerClusterServiceTopologyEndpointPort serverTopologyEndpoint = (HTTPServerClusterServiceTopologyEndpointPort) getTopologyEndpoint(names.getInteractAuditDMHTTPServerName());
        this.serverHostPort = serverTopologyEndpoint.getPortValue();
        this.serverHostName = serverTopologyEndpoint.getHostDNSName();
        if(serverTopologyEndpoint.isEncrypted()){
            this.hTTPScheme = "https";
        } else {
            this.hTTPScheme = "http";
        }
    }

    protected IPCTopologyEndpoint getTopologyEndpoint(String topologyEndpointName){
        getLogger().debug(".getTopologyEndpoint(): Entry, topologyEndpointName->{}", topologyEndpointName);
        ArrayList<TopologyNodeFDN> endpointFDNs = getProcessingPlant().getProcessingPlantNode().getEndpoints();
        for(TopologyNodeFDN currentEndpointFDN: endpointFDNs){
            IPCTopologyEndpoint endpointTopologyNode = (IPCTopologyEndpoint)getTopologyIM().getNode(currentEndpointFDN);
            if(endpointTopologyNode.getName().contentEquals(topologyEndpointName)){
                getLogger().debug(".getTopologyEndpoint(): Exit, node found -->{}", endpointTopologyNode);
                return(endpointTopologyNode);
            }
        }
        getLogger().debug(".getTopologyEndpoint(): Exit, Could not find node!");
        return(null);
    }

    protected OnExceptionDefinition getResourceNotFoundException() {
        OnExceptionDefinition exceptionDef = onException(ResourceNotFoundException.class)
                .handled(true)
                .log(LoggingLevel.INFO, "ResourceNotFoundException...")
                // use HTTP status 404 when data was not found
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(404))
                .setBody(simple("${exception.message}\n"));

        return(exceptionDef);
    }

    protected OnExceptionDefinition getResourceUpdateException() {
        OnExceptionDefinition exceptionDef = onException(ResourceUpdateException.class)
                .handled(true)
                .log(LoggingLevel.INFO, "ResourceUpdateException...")
                // use HTTP status 404 when data was not found
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setBody(simple("${exception.message}\n"));

        return(exceptionDef);
    }

    protected OnExceptionDefinition getGeneralException() {
        OnExceptionDefinition exceptionDef = onException(Exception.class)
                .handled(true)
                // use HTTP status 500 when we had a server side error
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setBody(simple("${exception.message}\n"));
        return (exceptionDef);
    }
}
