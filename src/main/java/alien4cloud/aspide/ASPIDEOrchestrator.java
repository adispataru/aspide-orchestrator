package alien4cloud.aspide;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

import alien4cloud.paas.exception.PluginConfigurationException;
import alien4cloud.paas.model.PaaSTopologyDeploymentContext;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import alien4cloud.model.orchestrators.locations.Location;
import alien4cloud.orchestrators.plugin.ILocationAutoConfigurer;
import alien4cloud.orchestrators.plugin.ILocationConfiguratorPlugin;
import alien4cloud.orchestrators.plugin.IOrchestratorPlugin;
import alien4cloud.orchestrators.plugin.model.PluginArchive;
import alien4cloud.paas.IPaaSCallback;
import alien4cloud.paas.model.PaaSDeploymentContext;
import lombok.extern.slf4j.Slf4j;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class ASPIDEOrchestrator extends ASPIDEProvider implements IOrchestratorPlugin<Configuration>, ILocationAutoConfigurer {

    @Inject
    private ASPIDELocationConfigurerFactory aspideLocationConfigurerFactory;

    @Override
    public ILocationConfiguratorPlugin getConfigurator(String locationType) {
        return aspideLocationConfigurerFactory.newInstance(locationType);
    }

    @Override
    public List<PluginArchive> pluginArchives() {

        return Collections.emptyList();
    }

    @Override
    public List<Location> getLocations() {
//        List<LocationSummary> locations = getNewBrooklynApi().getLocationApi().list();
        List<Location> newLocations = Lists.newArrayList();
        Location l = new Location();
        l.setName("Cluster");
        l.setInfrastructureType("ASPIDE");
        newLocations.add(l);
        return newLocations;
    }


    @Override
    public void setConfiguration(String s, Configuration configuration) throws PluginConfigurationException {
        super.configuration = configuration;
    }



    @Override
    public Set<String> init(Map<String, String> activeDeployments) {
        useLocalContextClassLoader();
        try {
            log.info("INIT: " + activeDeployments);

//            catalogMapper.addBaseTypes();

//            List<ToscaTypeProvider> metadataProviders = new LinkedList<>();
//            for (String providerClass : configuration.getProviders()) {
//                try {
//                    Object provider = beanFactory.getBean(Class.forName(providerClass));
//                    if(provider instanceof RequiresBrooklynApi) {
//                        ((RequiresBrooklynApi) provider).setBrooklynApi(getNewBrooklynApi());
//                    }
//                    // Alien UI has higher priority items at the end of the list.
//                    // Reverse the order here.
//                    metadataProviders.add(0, ToscaTypeProvider.class.cast(provider));
//                } catch (ClassNotFoundException e) {
//                    log.warn("Could not load metadata provider " + providerClass, e);
//                }
//            }

//            catalogMapper.mapBrooklynEntities(getNewBrooklynApi(), new ToscaMetadataProvider(metadataProviders));

        } finally {
            revertContextClassLoader();
        }
        return activeDeployments.keySet();
    }

    @Override
    public void update(PaaSTopologyDeploymentContext paaSTopologyDeploymentContext, IPaaSCallback<?> iPaaSCallback) {

    }

    @Override
    public void launchWorkflow(PaaSDeploymentContext paaSDeploymentContext, String s, Map<String, Object> map, IPaaSCallback<String> iPaaSCallback) {

    }
}
