package alien4cloud.aspide.model;

import java.util.ArrayList;
import java.util.List;

public class ASPIDETask {
    private String id;
    private DataDependency dataDependencies;
    private List<TaskDependency> taskDependencies;
    private String cpus;
    private String mem;
    private int numNodes;
    private String implementation;
    private String outputPath;

    public ASPIDETask(String id, DataDependency dataDependencies, List<TaskDependency> taskDependencies, String cpus, String mem, int numNodes, String implementation, String outputPath) {
        this.id = id;
        this.dataDependencies = dataDependencies;
        this.taskDependencies = taskDependencies;
        this.cpus = cpus;
        this.mem = mem;
        this.numNodes = numNodes;
        this.implementation = implementation;
        this.outputPath = outputPath;
    }

    public ASPIDETask() {

        this.taskDependencies = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DataDependency getDataDependencies() {
        return dataDependencies;
    }

    public void setDataDependencies(DataDependency dataDependencies) {
        this.dataDependencies = dataDependencies;
    }

//    public void addDataDependency(DataDependency dep) {
//        dataDependencies.add(dep);
//    }

    public void addTaskDependency(TaskDependency dep) {
        taskDependencies.add(dep);
    }

    public void setCpus(String cpus) {
        this.cpus = cpus;
    }

    public String getCpus() {
        return cpus;
    }

    public void setMem(String mem) {
        this.mem = mem;
    }

    public String getMem() {
        return mem;
    }

    public String getImplementation() {
        return implementation;
    }

    public void setImplementation(String implementation) {
        this.implementation = implementation;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }

    public List<TaskDependency> getTaskDependencies() {
        return taskDependencies;
    }

    public void setTaskDependencies(List<TaskDependency> taskDependencies) {
        this.taskDependencies = taskDependencies;
    }
}
