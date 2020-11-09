/*
 * Copyright 2017 Institute e-Austria Timisoara.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package alien4cloud.aspide;

import alien4cloud.aspide.model.*;
import alien4cloud.paas.function.FunctionEvaluator;
import alien4cloud.paas.model.PaaSNodeTemplate;
import alien4cloud.paas.model.PaaSTopology;
import alien4cloud.paas.model.PaaSTopologyDeploymentContext;
import com.google.common.collect.Maps;
import org.alien4cloud.tosca.model.definitions.*;
import org.alien4cloud.tosca.model.templates.Capability;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service for transformation of Alien PaaSTopologies into Marathon Groups and Apps definitions.
 * Currently only Docker containers are supported.
 *
 *
 * @author adrian
 *
 */
@Service
@Scope("prototype")
public class ASPIDEMappingService {

    /**
     * We allocate Service Ports starting from 31001
     */
    private AtomicInteger servicePortIncrement = new AtomicInteger(31001);

    /**
     * Map to store service ports allocated to marathon endpoints. Used to fulfill relationships requirements.
     */
    private Map<String, Integer> mapPortEndpoints = Maps.newHashMap();
    private Logger log = LoggerFactory.getLogger(ASPIDEMappingService.class);

    /**
     * Map an Alien deployment context to an ASPIDE workflow definition.
     *
     * @param deploymentContext the deployment to process
     * @return A Marathon CLGroup definition
     */
    public ASPIDEWorkflow buildWorkflowDefinition(PaaSTopologyDeploymentContext deploymentContext) {
        //Setting clGroup
        ASPIDEWorkflow workflow = new ASPIDEWorkflow();
        workflow.setId(deploymentContext.getDeploymentPaaSId());

        final List<PaaSNodeTemplate> computes =   deploymentContext.getPaaSTopology().getComputes();
        final List<PaaSNodeTemplate> tasks = deploymentContext.getPaaSTopology().getNonNatives();
        List<PaaSNodeTemplate> datanodes = tasks.stream().filter(node ->
                   node.getTemplate().getType().equals("aspide.nodes.Data") ||
                   node.getDerivedFroms().contains("aspide.nodes.Data"))
                .collect(Collectors.toList());

        tasks.removeAll(datanodes);

        tasks.forEach(node -> {

            if(node.getTemplate().getCapabilities().containsKey("host"))
                return;

            // Find data attached to the node
            List<String> dataInputsTargets = node.getRelationshipTemplates().stream()
                    .filter(rel -> rel.instanceOf("aspide.relationships.InputFrom"))
                    .map(x -> x.getTemplate().getTarget())
                    .collect(Collectors.toList());

            String hostRel = node.getRelationshipTemplates().stream()
                    .filter(rel -> rel.instanceOf("tosca.relationships.HostedOn"))
                    .map(x -> x.getTemplate().getTarget()).findFirst().orElse(null);
//                    .collect(Collectors.toList());

            final List<PaaSNodeTemplate> dataDeps = datanodes.stream()
                    .filter(nonNative -> dataInputsTargets.contains(nonNative.getId()))
                    .collect(Collectors.toList());
            final PaaSNodeTemplate host = computes.stream()
                    .filter(nonNative -> nonNative.getId().equals(hostRel)).findFirst().orElse(null);

            // Build the app definition and add it to the workflow
            if(host != null) {
                ASPIDETask task = buildAppDefinition(node, deploymentContext.getPaaSTopology(), host, dataDeps, workflow);
                workflow.getTasks().add(task);
            }
        });

        return workflow;
    }

    /**
     * Map an alien PaaSNodeTemplate to an ASPIDE Task Definition.
     *
     * @param node the node template to map
     * @param topology the topology the node belongs to
     * @param host
     * @param workflow
     * @return an ASPIDE Task definition
     */
    private ASPIDETask buildAppDefinition(PaaSNodeTemplate node, PaaSTopology topology, PaaSNodeTemplate host, List<PaaSNodeTemplate>
            dataNodes, ASPIDEWorkflow workflow) {
        final NodeTemplate nodeTemplate = node.getTemplate();
        //TODO Parse hosts, eventually
        /**
         * Init app structure
         */
//        App appDef = new App();
        ASPIDETask task = new ASPIDETask();

        ScalarPropertyValue v = (ScalarPropertyValue) host.getTemplate().getCapabilities().get("host").getProperties().get("num_cpus");
        if(v != null)
            task.setCpus(v.getValue());

        v = (ScalarPropertyValue) host.getTemplate().getCapabilities().get("host").getProperties().get("mem_size");
        if(v != null){
            task.setMem(v.getValue());
        }

        if(host.getTemplate().getType().equals("aspide.nodes.Carea")){
            String numNodes = ((ScalarPropertyValue) host.getTemplate().getProperties().get("numNodes")).getValue();
            task.setNumNodes(Integer.parseInt(numNodes));
        }else{
            task.setNumNodes(1);
        }
        task.setId(node.getId());

        /**
         * DCEX Program
         *
         */
        final Operation dcexOperation = node.getInterfaces().get("DCEX").getOperations().get("execute");

        final ImplementationArtifact implementationArtifact = dcexOperation.getImplementationArtifact();


        String artifactPath = implementationArtifact.getArtifactPath();
        String content = null;
        try {

            Scanner scan = new Scanner(new File(artifactPath));
            scan.useDelimiter("\\Z");
            content = scan.next();

            scan.close();
        } catch (FileNotFoundException e) {
            log.error("Cannot retrieve DCEX implementation artifact");
            e.printStackTrace();
        }

        if(content == null){
            log.error("Cannot retrieve DCEX implementation artifact.. Moving on without");
        }


        task.setImplementation(content);

        node.getRelationshipTemplates().stream()
                .filter(rel -> rel.instanceOf("aspide.relationships.InputFrom"))
                .forEach(x -> {
                    ScalarPropertyValue mountPath = (ScalarPropertyValue) x.getTemplate().getProperties().get("mount_path");
                    ScalarPropertyValue distrib = (ScalarPropertyValue) x.getTemplate().getProperties().get("data_distribution");
                    PaaSNodeTemplate dataNode = dataNodes.stream()
                            .filter(dn -> x.getTemplate().getTarget().equals(dn.getId()))
                            .findFirst().orElse(null);

                    if(dataNode!= null) {
                        ScalarPropertyValue id = (ScalarPropertyValue) dataNode.getTemplate().getProperties().get("id");

                        DataDependency dp = new DataDependency(id.getValue());
                        dp.setDataDistribution(distrib.getValue());
                        dp.setMountPath(mountPath.getValue());
                        task.setDataDependencies(dp);

                        /* when a task requires some data, check if data is produced by some other task.
                         * if yes, add a dependency between the two tasks.
                         */
                        dataNode.getRelationshipTemplates().stream()
                                .filter(rel -> rel.getTemplate().getType().equals("aspide.relationships.OutputTo"))
                                .forEach(rel -> {
                                    //if the task having as output this data is the same as the current one,
                                    // then it means the task is not dependant on itself,
                                    // but it will write to this path.
                                    if(rel.getSource().equals(task.getId()))
                                        return;

                                    //otherwise set a dependency between the data source and this task
                                    TaskDependency td = new TaskDependency(rel.getSource());
                                    task.addTaskDependency(td);
                                    workflow.getDependencies().putIfAbsent(node.getId(), new ArrayList<>());
                                    workflow.getDependencies().get(node.getId()).add(rel.getSource());
                                });

                    }

                });

        node.getRelationshipTemplates().stream()
                .filter(rel -> rel.instanceOf("aspide.relationships.OutputTo"))
                .forEach(rel -> {
                    ScalarPropertyValue outPath = (ScalarPropertyValue) rel.getTemplate().getProperties().get("path");
                    task.setOutputPath(outPath.getValue());

                });

        dataNodes.forEach(dnode -> {
            //also parse relationship to see if partition or replicate is selected
            Map<String, AbstractPropertyValue> props = dnode.getTemplate().getProperties();

        });


        return task;
    }

    public String retrieveValue(PaaSNodeTemplate paaSNodeTemplate, PaaSTopology paaSTopology, IValue val) {
        String value = ""; // TODO: This should be generalized into Alien parser
        if (val instanceof FunctionPropertyValue && "get_property".equals(((FunctionPropertyValue) val).getFunction())){
            if("REQ_TARGET".equals(((FunctionPropertyValue) val).getTemplateName())) {
                // Get property of a requirement's targeted capability
                value = getPropertyFromReqTarget(paaSNodeTemplate, paaSTopology, (FunctionPropertyValue) val);
            }else if("SELF".equals(((FunctionPropertyValue) val).getTemplateName())){
                // Get property of a capability
                value = getPropertyFromSelf(paaSNodeTemplate, paaSTopology, (FunctionPropertyValue) val);
            }
        } else if (val instanceof ScalarPropertyValue)
            value = ((ScalarPropertyValue) val).getValue();
        else if(val instanceof ConcatPropertyValue){
            StringBuilder sb = new StringBuilder("");
            for( IValue iValue : ((ConcatPropertyValue) val).getParameters()){
                sb.append(retrieveValue(paaSNodeTemplate, paaSTopology, iValue));
            }
            value = sb.toString();
        }
        return value;
    }

    public String retrieveValue(PaaSNodeTemplate paaSNodeTemplate, PaaSTopology paaSTopology, IValue val, TaskInfo info) {
        String value = ""; // TODO: This should be generalized into Alien parser
        if (val instanceof FunctionPropertyValue && "get_property".equals(((FunctionPropertyValue) val).getFunction())){
            if("REQ_TARGET".equals(((FunctionPropertyValue) val).getTemplateName())) {
                // Get property of a requirement's targeted capability
                value = getPropertyFromReqTarget(paaSNodeTemplate, paaSTopology, (FunctionPropertyValue) val, info);
            }else if("SELF".equals(((FunctionPropertyValue) val).getTemplateName())){
                // Get property of a capability
                value = getPropertyFromSelf(paaSNodeTemplate, paaSTopology, (FunctionPropertyValue) val, info);
            }
        } else if (val instanceof ScalarPropertyValue)
            value = ((ScalarPropertyValue) val).getValue();
        else if(val instanceof ConcatPropertyValue){
            StringBuilder sb = new StringBuilder("");
            for( IValue iValue : ((ConcatPropertyValue) val).getParameters()){
                sb.append(retrieveValue(paaSNodeTemplate, paaSTopology, iValue, info));
            }
            value = sb.toString();
        }
        return value;
    }

    /**
     * Search for a property of a capability being required as a target of a relationship.
     *
     * @param paaSNodeTemplate The source node of the relationships, wich defines the requirement.
     * @param paaSTopology the topology the node belongs to.
     * @param params the function parameters, e.g. the requirement name & property name to lookup.
     * @return a String representing the property value.
     */
    private String getPropertyFromReqTarget(PaaSNodeTemplate paaSNodeTemplate, PaaSTopology paaSTopology, FunctionPropertyValue params) {
        // Search for the requirement's target by filter the relationships' templates of this node.
        // If a target is found, then lookup for the given property name in its capabilities.
        // For Docker containers X Marathon, the orchestrator replaces the PORT and IP_ADDRESS by the target's service port and the load balancer hostname
        // respectively.

        String requirementName = params.getCapabilityOrRequirementName();
        String propertyName = params.getElementNameToFetch();

        return paaSNodeTemplate.getRelationshipTemplates().stream()
                .filter(item -> paaSNodeTemplate.getId().equals(item.getSource()) && requirementName.equals(item.getTemplate().getRequirementName()))
                .findFirst() // Find the first relationship template which fulfills the given requirement, for this source
                .map(relationshipTemplate -> {

                    if (relationshipTemplate.instanceOf("tosca.relationships.ConnectsTo")) { // - TODO/FIXME : check target derived from docker type
                        // Special marathon case: use service ports if the "Port" property is required.

                        if ("port".equalsIgnoreCase(propertyName))
                            // TODO: Retrieve service port if exists - if not, get capability value (for use cases where ports are statically defined)
                            return String.valueOf( // Service ports are mapped using the targetName + capabilityName
                                    mapPortEndpoints.getOrDefault(
                                            relationshipTemplate.getTemplate().getTarget() + relationshipTemplate.getTemplate().getTargetedCapabilityName(),
                                            0));
                        else if ("ip_address".equalsIgnoreCase(propertyName))
                            // TODO: If there is no service port, return <target_app_id>.marathon.mesos for DNS resolution

                            // Special marathon case: return marathon-lb hostname if an ip_address is required.
//                            return relationshipTemplate.getTemplate().getTarget() + ".marathon.mesos";
                            return "marathon-lb.marathon.mesos";
                    }
                    // Nominal case : get the requirement's targeted capability property.
                    // TODO: Add the REQ_TARGET keyword in the evaluateGetProperty function so this is evaluated at parsing
                    PaaSNodeTemplate target = paaSTopology.getAllNodes().get(relationshipTemplate.getTemplate().getTarget());
                    log.info(String.format("Target %s; property: %s", target.getTemplate().getName(),
                            target.getTemplate().getProperties()));
                    return ((ScalarPropertyValue) target.getTemplate().getProperties().get(propertyName)).getValue();
                }).orElse("");
    }

    private String getPropertyFromReqTarget(PaaSNodeTemplate paaSNodeTemplate, PaaSTopology paaSTopology, FunctionPropertyValue params, TaskInfo info) {
        // Search for the requirement's target by filter the relationships' templates of this node.
        // If a target is found, then lookup for the given property name in its capabilities.
        // For Docker containers X Marathon, the orchestrator replaces the PORT and IP_ADDRESS by the target's service port and the load balancer hostname
        // respectively.

        String requirementName = params.getCapabilityOrRequirementName();
        String propertyName = params.getElementNameToFetch();

        return paaSNodeTemplate.getRelationshipTemplates().stream()
                .filter(item -> paaSNodeTemplate.getId().equals(item.getSource()) && requirementName.equals(item.getTemplate().getRequirementName()))
                .findFirst() // Find the first relationship template which fulfills the given requirement, for this source
                .map(relationshipTemplate -> {

                    if (relationshipTemplate.instanceOf("tosca.relationships.ConnectsTo")) {

                        if ("port".equalsIgnoreCase(propertyName))
                            return info.getAttributes().get("ports");
                        else if ("ip_address".equalsIgnoreCase(propertyName))
                            return info.getAttributes().get("host");
                    }
                    // Nominal case : get the requirement's targeted capability property.
                    PaaSNodeTemplate target = paaSTopology.getAllNodes().get(relationshipTemplate.getTemplate().getTarget());
                    log.info(String.format("Target %s; property: %s", target.getTemplate().getName(),
                            target.getTemplate().getProperties()));
                    return ((ScalarPropertyValue) target.getTemplate().getProperties().get(propertyName)).getValue();

                }).orElse("");
    }

    public String getPropertyFromSelf(PaaSNodeTemplate paaSNodeTemplate, PaaSTopology paaSTopology, FunctionPropertyValue params) {
        // Search for the requirement's target by filter the relationships' templates of this node.
        // If a target is found, then lookup for the given property name in its capabilities.
        // For Docker containers X Marathon, the orchestrator replaces the PORT and IP_ADDRESS by the target's service port and the load balancer hostname
        // respectively.

        String capabilityName = params.getCapabilityOrRequirementName();
        String propertyName = params.getElementNameToFetch();

        return paaSNodeTemplate.getTemplate ().getCapabilities().entrySet().stream()
                .filter(item -> capabilityName.equals(item.getKey()))
                .findFirst() // Find the first relationship template which fulfills the given requirement, for this source
                .map(capabilityEntry -> {
                    Capability value = capabilityEntry.getValue();
                    // Special marathon case: use service ports if the "Port" property is required.
                    if ("port".equalsIgnoreCase(propertyName)) {
                        // TODO: Retrieve service port if exists - if not, get capability value (for use cases where ports are statically defined)
                        AbstractPropertyValue portProp = value.getProperties().get("port");
                        String port = "0";
                        if(portProp instanceof ScalarPropertyValue)
                            port = ((ScalarPropertyValue) portProp).getValue();
                        if(!port.equals("0")){
                            return port;
                        }else {
                            portProp = value.getProperties().get("docker_bridge_port_mapping");
                            if (portProp instanceof ScalarPropertyValue)
                                port = ((ScalarPropertyValue) portProp).getValue();
                            if (!port.equals("0")) {
                                return port;
                            }

                            return String.valueOf( // Service ports are mapped using the targetName + capabilityName
                                    mapPortEndpoints.getOrDefault(
                                            paaSNodeTemplate.getId().concat(capabilityEntry.getKey()), 0));
                        }
                    }
                    else if ("ip_address".equalsIgnoreCase(propertyName))
                        // TODO: If there is no service port, return <target_app_id>.marathon.mesos for DNS resolution

                        // Special marathon case: return marathon-lb hostname if an ip_address is required.
//                            return relationshipTemplate.getTemplate().getTarget() + ".marathon.mesos";
                        return "marathon-lb.marathon.mesos";

                    // Nominal case : get the requirement's targeted capability property.
                    // TODO: Add the REQ_TARGET keyword in the evaluateGetProperty function so this is evaluated at parsing
                    return FunctionEvaluator.evaluateGetPropertyFunction(params, paaSNodeTemplate, paaSTopology.getAllNodes());
                }).orElse("");
    }

    public String getPropertyFromSelf(PaaSNodeTemplate paaSNodeTemplate, PaaSTopology paaSTopology, FunctionPropertyValue params, TaskInfo info) {
        // Search for the requirement's target by filter the relationships' templates of this node.
        // If a target is found, then lookup for the given property name in its capabilities.
        // For Docker containers X Marathon, the orchestrator replaces the PORT and IP_ADDRESS by the target's service port and the load balancer hostname
        // respectively.

        String capabilityName = params.getCapabilityOrRequirementName();
        String propertyName = params.getElementNameToFetch();


        return paaSNodeTemplate.getTemplate ().getCapabilities().entrySet().stream()
                .filter(item -> capabilityName.equals(item.getKey()))
                .findFirst() // Find the first relationship template which fulfills the given requirement, for this source
                .map(capabilityEntry -> {
                    Capability value = capabilityEntry.getValue();
                    // Special marathon case: use service ports if the "Port" property is required.
                    if ("port".equalsIgnoreCase(propertyName)) {

                        if(value.getType().contains("aspide.capabilities.endpoint.DockerHOST")){
                            return ((ScalarPropertyValue) value.getProperties().get("port")).getValue();
                        }
                        return info.getAttributes().get("ports");
                    }
                    else if ("ip_address".equalsIgnoreCase(propertyName)) {

                        return info.getAttributes().get("host");
                    }
                    // Nominal case : get the requirement's targeted capability property.
                    // TODO: Add the REQ_TARGET keyword in the evaluateGetProperty function so this is evaluated at parsing
                    return FunctionEvaluator.evaluateGetPropertyFunction(params, paaSNodeTemplate, paaSTopology.getAllNodes());
                }).orElse("");
    }

}
