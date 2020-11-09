package alien4cloud.aspide.model;

import java.util.ArrayList;
import java.util.List;

public class TaskDependency extends Dependency {
    List<String> ports = new ArrayList<>();

    public TaskDependency(String id) {
        super(id);
    }

    public TaskDependency(String id, List<String> ports) {
        super(id);
        this.ports = ports;
    }

    public List<String> getPorts() {
        return ports;
    }

    public void setPorts(List<String> ports) {
        this.ports = ports;
    }
}
