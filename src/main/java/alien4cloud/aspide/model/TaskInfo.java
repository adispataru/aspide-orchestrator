package alien4cloud.aspide.model;

import java.util.Map;

public class TaskInfo {
    private String id;
    private String state;
    private Map<String, String> attributes;

    public TaskInfo(String id, String state, Map<String, String> attributes) {
        this.id = id;
        this.state = state;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}
