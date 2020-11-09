package alien4cloud.aspide;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.alien4cloud.tosca.model.definitions.PropertyDefinition;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import alien4cloud.model.orchestrators.ArtifactSupport;
import alien4cloud.model.orchestrators.locations.LocationSupport;
import alien4cloud.orchestrators.plugin.IOrchestratorPluginFactory;

@Slf4j
@Component("aspide-orchestrator-factory")
public class ASPIDEOrchestratorFactory implements IOrchestratorPluginFactory<ASPIDEOrchestrator, Configuration> {

    public static final String TYPE = "ASPIDE";

    @Autowired
    private BeanFactory beanFactory;


//    @Override
//    public ASPIDEOrchestrator newInstance() {
//        ASPIDEOrchestrator instance = beanFactory.getBean(ASPIDEOrchestrator.class);
//        log.info("Init ASPIDE provider and beanFactory is {}", beanFactory);
//        return instance;
//    }

    @Override
    public ASPIDEOrchestrator newInstance(Configuration configuration) {
        ASPIDEOrchestrator instance = beanFactory.getBean(ASPIDEOrchestrator.class);
        log.info("Init ASPIDE provider and beanFactory is {}", beanFactory);
        return instance;
    }

    @Override
    public void destroy(ASPIDEOrchestrator instance) {
        log.info("DESTROYING (noop)", instance);
    }

    @Override
    public Class<Configuration> getConfigurationType() {
        return Configuration.class;
    }

    @Override
    public Configuration getDefaultConfiguration() {
        // List ordered lowest to highest priority.
        List<String> metadataProviders = new ArrayList<>();
//        MutableList.of(
//                ToscaMeta
//                DefaultToscaTypeProvider.class.getName(),
//                BrooklynToscaTypeProvider.class.getName());
        return new Configuration("10.10.10.3", "aspide", "aspide", "/home/aspide/dce", "ASPIDE", metadataProviders);
    }

    @Override
    public LocationSupport getLocationSupport() {
        return new LocationSupport(true, new String[] {TYPE});
    }

    @Override
    public ArtifactSupport getArtifactSupport() {
        return new ArtifactSupport();
    }

    @Override
    public Map<String, PropertyDefinition> getDeploymentPropertyDefinitions() {
        return null;
    }

    @Override
    public String getType() {
        return "Compute";
    }

}
