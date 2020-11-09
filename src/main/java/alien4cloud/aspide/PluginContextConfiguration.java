package alien4cloud.aspide;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Plugin spring configuration entry point.
 */
@Configuration
@ComponentScan(basePackages = {"alien4cloud.aspide", "alien4cloud.aspide.artifacts"})
public class PluginContextConfiguration {

//    @Bean
//    public ToscaTypeProvider defaultToscaTypeProvider() {
//        return new DefaultToscaTypeProvider();
//    }

//    @Bean
//    public BrooklynToscaTypeProvider brooklynToscaTypeProvider() {
//        return new BrooklynToscaTypeProvider();
//    }
}