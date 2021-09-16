package net.fhirfactory.pegacorn.hestia.audit.dm.processingplant.configuration;

import net.fhirfactory.pegacorn.deployment.properties.configurationfilebased.common.segments.ports.interact.ClusteredInteractHTTPServerPortSegment;
import net.fhirfactory.pegacorn.deployment.properties.configurationfilebased.common.segments.ports.interact.StandardInteractClientPortSegment;
import net.fhirfactory.pegacorn.deployment.topology.factories.archetypes.fhirpersistence.im.FHIRIMSubsystemTopologyFactory;
import net.fhirfactory.pegacorn.deployment.topology.model.nodes.*;
import net.fhirfactory.pegacorn.deployment.topology.model.nodes.common.EndpointProviderInterface;
import net.fhirfactory.pegacorn.hestia.audit.dm.common.HestiaAuditDMNames;
import net.fhirfactory.pegacorn.util.PegacornEnvironmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class HestiaAuditDMTopologyFactory extends FHIRIMSubsystemTopologyFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HestiaAuditDMTopologyFactory.class);

    @Inject
    private HestiaAuditDMNames hestiaDMNames;

    @Inject
    private PegacornEnvironmentProperties pegacornEnvironmentProperties;

    @Override
    protected Logger specifyLogger() {
        return (LOG);
    }

    @Override
    protected Class specifyPropertyFileClass() {
        return (HestiaAuditDMConfigurationFile.class);
    }

    @Override
    protected ProcessingPlantTopologyNode buildSubsystemTopology() {
        SubsystemTopologyNode subsystemTopologyNode = addSubsystemNode(getTopologyIM().getSolutionTopology());
        BusinessServiceTopologyNode businessServiceTopologyNode = addBusinessServiceNode(subsystemTopologyNode);
        DeploymentSiteTopologyNode deploymentSiteTopologyNode = addDeploymentSiteNode(businessServiceTopologyNode);
        ClusterServiceTopologyNode clusterServiceTopologyNode = addClusterServiceNode(deploymentSiteTopologyNode);

        PlatformTopologyNode platformTopologyNode = addPlatformNode(clusterServiceTopologyNode);
        ProcessingPlantTopologyNode processingPlantTopologyNode = addPegacornProcessingPlant(platformTopologyNode);
        addPrometheusPort(processingPlantTopologyNode);
        addJolokiaPort(processingPlantTopologyNode);
        addKubeLivelinessPort(processingPlantTopologyNode);
        addKubeReadinessPort(processingPlantTopologyNode);
        addEdgeAnswerPort(processingPlantTopologyNode);
        addIntraZoneIPCJGroupsPort(processingPlantTopologyNode);
        addInterZoneIPCJGroupsPort(processingPlantTopologyNode);

        // Unique to HestiaDM
        getLogger().trace(".buildSubsystemTopology(): Add the httpFHIRServer port to the ProcessingPlant Topology Node");
        addHTTPServerPorts(processingPlantTopologyNode);
        return(processingPlantTopologyNode);
    }

    protected void addHTTPServerPorts( EndpointProviderInterface endpointProvider) {
        getLogger().debug(".addHTTPClientPorts(): Entry, endpointProvider->{}", endpointProvider);

        getLogger().trace(".addHTTPClientPorts(): Creating the HTTP Client (Used to Connect-To Hestia Audit DM)");
        ClusteredInteractHTTPServerPortSegment interactHTTPServer = ((HestiaAuditDMConfigurationFile) getPropertyFile()).getInteractHestiaDMHTTPServer();
        newHTTPServer(endpointProvider, hestiaDMNames.getInteractAuditDMHTTPServerName(), interactHTTPServer );

        getLogger().debug(".addHTTPClientPorts(): Exit");
    }

    protected String specifyPropertyFileName() {
        LOG.info(".specifyPropertyFileName(): Entry");
        String configurationFileName = pegacornEnvironmentProperties.getMandatoryProperty("DEPLOYMENT_CONFIG_FILE");
        if(configurationFileName == null){
            throw(new RuntimeException("Cannot load configuration file!!!! (SUBSYSTEM-CONFIG_FILE="+configurationFileName+")"));
        }
        LOG.info(".specifyPropertyFileName(): Exit, filename->{}", configurationFileName);
        return configurationFileName;
    }
}
