/*
 * The MIT License
 *
 * Copyright 2021 Mark A. Hunter (ACT Health).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.edge.answer;


import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.MethodOutcome;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.fhirfactory.pegacorn.components.capabilities.CapabilityFulfillmentInterface;
import net.fhirfactory.pegacorn.components.capabilities.base.CapabilityUtilisationRequest;
import net.fhirfactory.pegacorn.components.capabilities.base.CapabilityUtilisationResponse;
import net.fhirfactory.pegacorn.components.interfaces.topology.ProcessingPlantInterface;
import net.fhirfactory.pegacorn.components.transaction.model.SimpleResourceID;
import net.fhirfactory.pegacorn.components.transaction.model.SimpleTransactionOutcome;
import net.fhirfactory.pegacorn.components.transaction.valuesets.TransactionStatusEnum;
import net.fhirfactory.pegacorn.components.transaction.valuesets.TransactionTypeEnum;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.AuditEventProxy;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.StoreAuditOutcomeEnum;
import net.fhirfactory.pegacorn.util.FHIRContextUtility;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.hl7.fhir.r4.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Mark A. Hunter
 */

@ApplicationScoped
public class AuditEventJGroupsServer extends RouteBuilder implements CapabilityFulfillmentInterface{
    private static final Logger LOG = LoggerFactory.getLogger(AuditEventJGroupsServer.class);
    
    private boolean initialised;
    private ObjectMapper jsonMapper;
    private IParser fhirParser;
    
    @Inject
    private ProcessingPlantInterface processingPlant;
    
    @Inject
    private AuditEventProxy auditEventProxy;
    
    @Inject
    private FHIRContextUtility fhirContextUtility;
    
    //
    // Constructor(s)
    //
    
    public AuditEventJGroupsServer(){
        super();
        setInitialised(false);
        jsonMapper = new ObjectMapper();
    }
    
    //
    // Post Construct
    //
    
        //
    // Post Constructor(s)
    //
    
    @PostConstruct
    public void initialise(){
        getLogger().debug(".initialise(): Entry");
        if(isInitialised()){
            getLogger().debug(".initialise(): Exit, already initialised!");
        } else {
            fhirParser = fhirContextUtility.getJsonParser();
            setInitialised(true);
        }
    }
    
    //
    // Business Methods
    //
    
    @Override
    public CapabilityUtilisationResponse executeTask(CapabilityUtilisationRequest request) {
        getLogger().debug(".executeTask(): Entry, request->{}", request);
        String capability = request.getRequiredCapabilityName();
        switch(capability){
            case "FHIR-AuditEvent-Persistence": {
                CapabilityUtilisationResponse capabilityUtilisationResponse = executeWriteAuditEventTask(request);
                return (capabilityUtilisationResponse);
            }
            default:{
                CapabilityUtilisationResponse response = new CapabilityUtilisationResponse();
                response.setDateCompleted(Instant.now());
                response.setSuccessful(false);
                response.setAssociatedRequestID(request.getRequestID());
                return(response);
            }
        }
    }
    
    private CapabilityUtilisationResponse executeWriteAuditEventTask(CapabilityUtilisationRequest request){
        getLogger().debug(".executeWriteAuditEventTask(): Entry, request->{}", request);
        
        //
        // Get the JSON payload (AuditEvent as JSON)
        String auditEventAsString = request.getRequestContent();
        
        //
        // Convert to AuditEvent
        AuditEvent auditEvent = (AuditEvent) fhirParser.parseResource(auditEventAsString);
        
        //
        // Write the AuditEvent
        StoreAuditOutcomeEnum createEventOutcome = getAuditEventProxy().createEvent(auditEvent);
        
        // Kelly, the above response is wrong. You need to respond with a MethodOutcome object.  

        String simpleOutcomeAsString = null;
        SimpleTransactionOutcome simpleOutcome = new SimpleTransactionOutcome();
        SimpleResourceID resourceID = new SimpleResourceID();
        if(methodOutcome.getCreated()) {
            if(methodOutcome.getId() != null) {
                if (methodOutcome.getId().hasResourceType()) {
                    resourceID.setResourceType(methodOutcome.getId().getResourceType());
                } else {
                    resourceID.setResourceType("AuditEvent");
                }
                resourceID.setValue(methodOutcome.getId().getValue());
                if (methodOutcome.getId().hasVersionIdPart()) {
                    resourceID.setVersion(methodOutcome.getId().getVersionIdPart());
                } else {
                    resourceID.setVersion(SimpleResourceID.DEFAULT_VERSION);
                }
                simpleOutcome.setResourceID(resourceID);
            }
            simpleOutcome.setTransactionStatus(TransactionStatusEnum.CREATION_FINISH);
        } else {
            simpleOutcome.setTransactionStatus(TransactionStatusEnum.CREATION_FAILURE);
        }
        simpleOutcome.setTransactionType(TransactionTypeEnum.CREATE);
        simpleOutcome.setTransactionSuccessful(methodOutcome.getCreated());
        try {
            simpleOutcomeAsString = jsonMapper.writeValueAsString(simpleOutcome);
        } catch (JsonProcessingException e) {
            getLogger().warn(".executeWriteAuditEventTask(): Cannot convert MethodOutcome to string, error->",e);
        }
        CapabilityUtilisationResponse response = new CapabilityUtilisationResponse();
        if(simpleOutcomeAsString != null){
            response.setResponseContent(simpleOutcomeAsString);
            response.setSuccessful(true);
        } else {
            response.setSuccessful(false);
        }
        response.setDateCompleted(Instant.now());
        response.setAssociatedRequestID(request.getRequestID());
        return(response);
    }

       
    private boolean shouldPersistAuditEvent(){
        String parameterValue = getProcessingPlant().getProcessingPlantNode().getOtherConfigurationParameter("AUDIT_EVENT_PERSISTENCE");
        if(parameterValue != null){
            if(parameterValue.equalsIgnoreCase("true")){
                return(true);
            }
        } else {
            return(false);
        }
        return(false);
    }
    
    
    @Override
    public void configure() throws Exception {
        String processingPlantName = getClass().getSimpleName();

        from("timer://"+processingPlantName+"?delay=1000&repeatCount=1")
            .routeId("ProcessingPlant::"+processingPlantName)
            .log(LoggingLevel.DEBUG, "Starting....");
    }
    
    
    //
    // Getters (and Setters)
    //

    protected static Logger getLogger() {
        return LOG;
    }

    public boolean isInitialised() {
        return initialised;
    }

    protected void setInitialised(boolean initialisationStatus){
        this.initialised = initialisationStatus;
    }
    
    protected ProcessingPlantInterface getProcessingPlant(){
        return(processingPlant);
    }
    
    protected AuditEventProxy getAuditEventProxy(){
        return(auditEventProxy);
    }
    

}
