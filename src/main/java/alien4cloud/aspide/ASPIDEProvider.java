package alien4cloud.aspide;

import alien4cloud.application.ApplicationService;
import alien4cloud.aspide.model.ASPIDETask;
import alien4cloud.aspide.model.ASPIDEWorkflow;
import alien4cloud.aspide.model.TaskInfo;
import alien4cloud.dao.IGenericSearchDAO;
import alien4cloud.orchestrators.locations.services.LocationService;
import alien4cloud.paas.IConfigurablePaaSProvider;
import alien4cloud.paas.IPaaSCallback;
import alien4cloud.paas.exception.MaintenanceModeException;
import alien4cloud.paas.exception.OperationExecutionException;
import alien4cloud.paas.model.*;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.newHashMap;

public abstract class ASPIDEProvider implements IConfigurablePaaSProvider<Configuration> {
    private static final Logger log = LoggerFactory.getLogger(ASPIDEProvider.class);

    protected Configuration configuration;




    protected Map<String,PaaSTopologyDeploymentContext> knownDeployments = Maps.newConcurrentMap();
    protected Map<String, Optional<DeploymentStatus>> deploymentStatuses = Maps.newConcurrentMap();
    protected Map<String, String> SLURM_JOB_IDS = Maps.newConcurrentMap();

    @Autowired
    private ApplicationService applicationService;

    @Autowired
    private EventService eventService;

    @Autowired
    private LocationService locationService;

    @Autowired
    @Qualifier("alien-es-dao")
    private IGenericSearchDAO alienDAO;

    @Autowired
    private BeanFactory beanFactory;

    @Autowired
    private ASPIDEOrcClient slurmClient;

    @Autowired
    private ASPIDEMappingService aspideMappingService;

    ThreadLocal<ClassLoader> oldContextClassLoader = new ThreadLocal<ClassLoader>();
    private Map<String, ASPIDEWorkflow> aspideDeployments = Maps.newConcurrentMap();


    protected void useLocalContextClassLoader() {
        if (oldContextClassLoader.get()==null) {
            oldContextClassLoader.set( Thread.currentThread().getContextClassLoader() );
        }
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    }
    protected void revertContextClassLoader() {
        if (oldContextClassLoader.get()==null) {
            log.warn("No local context class loader to revert");
        }
        Thread.currentThread().setContextClassLoader(oldContextClassLoader.get());
        oldContextClassLoader.remove();
    }


    @Override
    @SneakyThrows
    public void deploy(PaaSTopologyDeploymentContext deploymentContext, IPaaSCallback<?> callback) {
        log.info("DEPLOY "+deploymentContext+" / "+callback);
        String topologyId = deploymentContext.getDeploymentTopology().getId();
        String topologyVer = deploymentContext.getDeploymentTopology().getVersionId();
        String appId = topologyVer.split(":")[0];


        ASPIDEWorkflow workflow = aspideMappingService.buildWorkflowDefinition(deploymentContext);



        knownDeployments.put(deploymentContext.getDeploymentId(), deploymentContext);

        deploymentStatuses.put(deploymentContext.getDeploymentId(), Optional.of(DeploymentStatus.DEPLOYMENT_IN_PROGRESS));
        PaaSDeploymentStatusMonitorEvent updatedStatus = new PaaSDeploymentStatusMonitorEvent();
        updatedStatus.setDeploymentId(deploymentContext.getDeploymentId());
        updatedStatus.setDeploymentStatus(DeploymentStatus.DEPLOYMENT_IN_PROGRESS);
        eventService.registerEvent(new Date(System.currentTimeMillis()), updatedStatus);
        boolean deployed = startWorkflow(deploymentContext, workflow);
        if(deployed) {
            deploymentStatuses.put(deploymentContext.getDeploymentId(), Optional.of(DeploymentStatus.DEPLOYED));
            aspideDeployments.put(deploymentContext.getDeploymentId(), workflow);
            if (callback != null) callback.onSuccess(null);
        }else{
//            knownDeployments.remove(deploymentContext.getDeploymentId());
            aspideDeployments.remove(deploymentContext.getDeploymentId());
            deploymentStatuses.put(deploymentContext.getDeploymentId(), Optional.of(DeploymentStatus.UNDEPLOYED));

            PaaSDeploymentStatusMonitorEvent status = new PaaSDeploymentStatusMonitorEvent();
            status.setDeploymentId(deploymentContext.getDeploymentId());
            status.setDeploymentStatus(DeploymentStatus.UNDEPLOYED);
            eventService.registerEvent(new Date(System.currentTimeMillis()), status);

            if (callback != null) callback.onFailure (new Exception("Cannot start workflow"));
        }
    }



    private boolean startWorkflow(PaaSTopologyDeploymentContext paaSTopologyDeploymentContext, ASPIDEWorkflow workflow) {
        Map<String, Object> deployedApps = new HashMap<>();


        //create slurm script
        StringBuilder sb = new StringBuilder("#!/bin/bash\n");
        sb.append(String.format("#SBATCH -J %s\n", workflow.getId()));



        List<ASPIDETask> finalTasks = new ArrayList<>();
        Map<String, List<String>> dependencies = new HashMap<>();
        for(String x : workflow.getDependencies().keySet()){
            dependencies.put(x, new ArrayList<>());
            dependencies.get(x).addAll(workflow.getDependencies().get(x));
        }

        Queue<ASPIDETask> Q = new LinkedList<>(workflow.getTasks());
        while(!Q.isEmpty()){
            ASPIDETask t = Q.remove();
            if(dependencies.get(t.getId()) != null && dependencies.get(t.getId()).size() > 0) {
                Q.add(t);

            }else{
                finalTasks.add(t);
                for(String x : dependencies.keySet()){
                    dependencies.get(x).remove(t.getId());
                }
            }
        }

        AtomicInteger numNodes = new AtomicInteger(0);
        AtomicInteger memory = new AtomicInteger(0);
        finalTasks.forEach(t -> {


            if(workflow.getDependencies().containsKey(t.getId())){
                numNodes.set(Math.max(numNodes.get(), t.getNumNodes()));
                if(t.getMem() != null) {
                    int mem = computeMemoryMB(t.getMem());
                    memory.set(Math.max(memory.get(), mem));
                }
            }else{
                numNodes.addAndGet(t.getNumNodes());
                if(t.getMem() != null) {

                    int mem = computeMemoryMB(t.getMem());
                    memory.addAndGet(mem);
                }

            }
        });


        sb.append(String.format("#SBATCH -N %d\n", numNodes.get()));
        sb.append(String.format("#SBATCH --mem=%dmb\n", memory.get()));

        int N = finalTasks.size();
        workflow.setTasks(finalTasks);

        String moduleLoads = "\n";
        sb.append(moduleLoads);

        for(int i = 0; i < N; i++){
            ASPIDETask t = finalTasks.get(i);
            String taskName = t.getId();
            String impl = t.getImplementation();
            slurmClient.compileImplementationSSH(configuration, workflow.getId(), taskName, impl);
            Map<String, String> attrs = new HashMap<>();
            attrs.put("SLURM_ID", String.valueOf(i));
            workflow.getTaskInformation().put(t.getId(), new TaskInfo(t.getId(), "DEPLOYING", attrs));

            if(t.getMem()!= null) {
                sb.append(String.format("srun -n %d --mem=%dmb ./%s", t.getNumNodes(), computeMemoryMB(t.getMem()), taskName));
            }else{
                sb.append(String.format("srun -n %d ./%s", t.getNumNodes(), taskName));
            }

            if(i < N - 1){
                ASPIDETask nextTask = finalTasks.get(i+1);
                boolean depends = false;
                for(int j = 0; j <= i; j++) {
                    List<String> deps = workflow.getDependencies().get(nextTask.getId());
                    if (deps != null && deps.contains(finalTasks.get(j).getId())) {
                        //next task is dependent on this one
                        depends = true;
                        break;
                    }
                }
                if(!depends){
                    sb.append(" & ");
                }

            }
            sb.append("\n");

        }

        slurmClient.copySLURMScript(configuration, workflow.getId(), sb.toString());

        String sBatch = slurmClient.sBatch(configuration, workflow.getId());

        PaaSDeploymentStatusMonitorEvent updatedStatus = new PaaSDeploymentStatusMonitorEvent();
        if(sBatch.contains("Submitted batch job")) {
            String jobNumber = sBatch.split("\\s+")[3];
            SLURM_JOB_IDS.put(workflow.getId(), jobNumber);


            updatedStatus.setDeploymentId(paaSTopologyDeploymentContext.getDeploymentId());
            updatedStatus.setDeploymentStatus(DeploymentStatus.DEPLOYED);

            eventService.registerEvent(new Date(System.currentTimeMillis()), updatedStatus);
            return true;
        }else{

            updatedStatus.setDeploymentId(paaSTopologyDeploymentContext.getDeploymentId());
            updatedStatus.setDeploymentStatus(DeploymentStatus.UNDEPLOYED);
            eventService.registerEvent(new Date(System.currentTimeMillis()), updatedStatus);
            return false;
        }

    }

    private int computeMemoryMB(String mem) {
        String[] tokens = mem.split(" ");
        int size = Integer.parseInt(tokens[0]);
        String type = tokens[1];
        int k = 1024;
        if(type.equalsIgnoreCase("mb"))
            return size;
        else if (type.equalsIgnoreCase("gb"))
            return size * k;
        else if (type.equalsIgnoreCase("tb"))
            return size * k * k;
        return size;
    }


    @Override
    public void undeploy(PaaSDeploymentContext deploymentContext, IPaaSCallback<?> callback) {
//        log.info("UNDEPLOY " + deploymentContext + " / " + callback);
        ASPIDEWorkflow workflow = aspideDeployments.get(deploymentContext.getDeploymentId());

        if(workflow != null) {
            slurmClient.cancelJob(configuration, SLURM_JOB_IDS.get(workflow.getId()));

            //TODO clean folder?

        }
        String deploymentId = deploymentContext.getDeploymentId();
        aspideDeployments.remove(deploymentId);
        this.deploymentStatuses.put(deploymentId, Optional.fromNullable(DeploymentStatus.UNDEPLOYED));

//        knownDeployments.remove(deploymentContext.getDeploymentId());
        PaaSDeploymentStatusMonitorEvent updatedStatus = new PaaSDeploymentStatusMonitorEvent();
        updatedStatus.setDeploymentId(deploymentId);
        updatedStatus.setDeploymentStatus(DeploymentStatus.UNDEPLOYED);
        eventService.registerEvent(new Date(System.currentTimeMillis()), updatedStatus);
        if (callback != null) {
            callback.onSuccess(null);
        }
    }

    @Override
    public void scale(PaaSDeploymentContext deploymentContext, String nodeTemplateId, int instances, IPaaSCallback<?> callback) {
        log.warn("SCALE not supported");
    }

    @Override
    public void getStatus(PaaSDeploymentContext deploymentContext, IPaaSCallback<DeploymentStatus> callback) {
        if (callback!=null) {
            String entityId = deploymentContext.getDeploymentId();
            log.info("GET STATUS - " + entityId);
            Optional<DeploymentStatus> deploymentStatus = deploymentStatuses.get(entityId);
            DeploymentStatus status = DeploymentStatus.UNDEPLOYED;
            if(aspideDeployments.get(deploymentContext.getDeploymentId()) != null){
                status = DeploymentStatus.DEPLOYED;
            }
            callback.onSuccess(deploymentStatus.isPresent() ? deploymentStatus.get() : DeploymentStatus.UNDEPLOYED);
        }
    }

    @Override
    public void getInstancesInformation(PaaSTopologyDeploymentContext deploymentContext,
            IPaaSCallback<Map<String, Map<String, InstanceInformation>>> callback) {


        ASPIDEWorkflow workflow = aspideDeployments.get(deploymentContext.getDeploymentId());

        if(workflow != null) {
            Map<String, Map<String, InstanceInformation>> topology = Maps.newHashMap();
            final List<PaaSNodeTemplate> computes =   deploymentContext.getPaaSTopology().getComputes();
            final List<PaaSNodeTemplate> tasks = deploymentContext.getPaaSTopology().getNonNatives();
            List<PaaSNodeTemplate> datanodes = tasks.stream().filter(node ->
                    node.getTemplate().getType().equals("aspide.nodes.Data") ||
                            node.getDerivedFroms().contains("aspide.nodes.Data"))
                    .collect(Collectors.toList());

            tasks.removeAll(datanodes);

            computes.forEach(node -> {
                if(SLURM_JOB_IDS.get(workflow.getId()) != null){
                    Map<String, InstanceInformation> attrs = new HashMap<>();

                    InstanceInformation ii = new InstanceInformation("success", InstanceStatus.SUCCESS, newHashMap(), newHashMap(), newHashMap());
                    attrs.put(node.getId(), ii);

                    topology.put(node.getId(), attrs);
                }else{
                    Map<String, InstanceInformation> attrs = new HashMap<>();

                    InstanceInformation ii = new InstanceInformation("processing", InstanceStatus.PROCESSING, newHashMap(), newHashMap(), newHashMap());
                    attrs.put(node.getId(), ii);

                    topology.put(node.getId(), attrs);
                }
            });

            for (ASPIDETask task : workflow.getTasks()) {
                // We lookup Entities based on tosca.id (getDeploymentId())
//            String appId = deploymentContext.getDeployment().getOrchestratorDeploymentId();
                TaskInfo taskInfo = workflow.getTaskInformation().get(task.getId());
                String slurmId = taskInfo.getAttributes().get("SLURM_ID");

                Map<String, InstanceInformation> instancesInfo = newHashMap();

                PaaSNodeTemplate nodeTemplate = tasks.stream()
                        .filter(p -> p.getId().equals(taskInfo.getId()))
                        .findFirst().orElse(null);
                if (nodeTemplate == null) {
                    continue;
                }


                Map<String, String> attr = new HashMap<>();
                attr.put("slurmJobID", SLURM_JOB_IDS.get(workflow.getId()));
                attr.put("slurmStep", slurmId);
                if(!taskInfo.getAttributes().containsKey("slurmJobID")){
                    taskInfo.getAttributes().putAll(attr);
                }

//                TaskInfo info = new TaskInfo(task.getId(), InstanceStatus.SUCCESS.name(), attr);
//                info.getAttributes().putAll(attr);
                final InstanceInformation ins = this.getInstanceInformation(workflow.getId(), taskInfo);
                instancesInfo.put(taskInfo.getId(), ins);
//                        });

//                if(!taskInfo.getState().equals(ins.getState())) {
                    PaaSInstanceStateMonitorEvent istat = new PaaSInstanceStateMonitorEvent();
                    istat.setInstanceId(task.getId());
                    istat.setNodeTemplateId(nodeTemplate.getId());
                    istat.setInstanceState(ins.getState());
                    istat.setInstanceStatus(ins.getInstanceStatus());
                    istat.setAttributes(ins.getAttributes());
                    istat.setRuntimeProperties(ins.getRuntimeProperties());
                    istat.setDeploymentId(deploymentContext.getDeploymentId());

                    eventService.registerEvent(new Date(System.currentTimeMillis()), istat);
                    workflow.getTaskInformation().put(task.getId(), taskInfo);
                    taskInfo.getAttributes().putAll(ins.getRuntimeProperties());
                    taskInfo.setState(ins.getState());
//                }


                topology.put(nodeTemplate.getId(), instancesInfo);

                PaaSRelationshipTemplate outputRel = nodeTemplate.getRelationshipTemplates().stream()
                        .filter(rel -> rel.instanceOf("aspide.relationships.OutputTo") || rel.instanceOf("aspide.relationships.InputFrom"))
                        .findFirst().orElse(null);
                if(outputRel != null) {

                    PaaSNodeTemplate outNode = datanodes.stream()
                            .filter(p -> p.getId().equals(outputRel.getTemplate().getTarget()))
                            .findFirst().orElse(null);
                    if(outNode != null){
                        Map<String, InstanceInformation> attrs = new HashMap<>();

                        InstanceInformation ii = new InstanceInformation(ins.getState(), ins.getInstanceStatus(), newHashMap(), newHashMap(), newHashMap());
                        attrs.put(outNode.getId(), ii);

                        topology.put(outNode.getId(), attrs);
                    }
                }






            }

            callback.onSuccess(topology);
        }
    }


    /**
     * Get instance information, eg. status and runtime properties, from an ASPIDE TaskInfo.
     * @param info A @TaskInfo object
     * @return An InstanceInformation
     */
    private InstanceInformation getInstanceInformation(String id, TaskInfo info) {
        Map<String, String> runtimeProps = newHashMap(info.getAttributes());


        InstanceStatus instanceStatus = InstanceStatus.PROCESSING;
        String state = "processing";

        Map<String, String> slurmInfo = slurmClient.getInfo(configuration, SLURM_JOB_IDS.get(id), info.getAttributes().get("SLURM_ID"));



        if(slurmInfo != null){
            runtimeProps.putAll(slurmInfo);
            if(slurmInfo.get("status") != null){
                state = slurmInfo.get("status");
                instanceStatus = InstanceStatus.valueOf(state);
            }
        }



        return new InstanceInformation(state, instanceStatus, newHashMap(), runtimeProps, newHashMap());
    }

    @Override
    public synchronized void getEventsSince(Date date, int maxEvents, IPaaSCallback<AbstractMonitorEvent[]> eventCallback) {
        eventService.getEventsSince(date, maxEvents, eventCallback);
    }

    @Override
    public void executeOperation(PaaSTopologyDeploymentContext deploymentContext, NodeOperationExecRequest request,
            IPaaSCallback<Map<String, String>> operationResultCallback) throws OperationExecutionException {
        log.warn("EXEC OP not supported: " + request);

    }

    @Override
    public void switchMaintenanceMode(PaaSDeploymentContext deploymentContext, boolean maintenanceModeOn) throws MaintenanceModeException {
        log.info("MAINT MODE (ignored): " + maintenanceModeOn);
    }

    @Override
    public void switchInstanceMaintenanceMode(PaaSDeploymentContext deploymentContext, String nodeId, String instanceId, boolean maintenanceModeOn)
            throws MaintenanceModeException {
        log.info("MAINT MODE for INSTANCE (ignored): " + maintenanceModeOn);
    }

}
