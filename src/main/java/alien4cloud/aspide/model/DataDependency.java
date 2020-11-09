package alien4cloud.aspide.model;

public class DataDependency extends Dependency {
    private String mountPath;
    private String dataDistribution;
    public DataDependency(String id) {
        super(id);
    }

    public DataDependency(String id, String mountPath, String dataDistribution) {
        super(id);
        this.mountPath = mountPath;
        this.dataDistribution = dataDistribution;
    }

    public String getMountPath() {
        return mountPath;
    }

    public void setMountPath(String mountPath) {
        this.mountPath = mountPath;
    }

    public String getDataDistribution() {
        return dataDistribution;
    }

    public void setDataDistribution(String dataDistribution) {
        this.dataDistribution = dataDistribution;
    }
}
