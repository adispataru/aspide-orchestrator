package alien4cloud.aspide;


import lombok.extern.slf4j.Slf4j;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = "classpath:test-context.xml", classes = {TestConfiguration.class})
@Slf4j
public class PluginTest {

    @Test
    public void testPlugin() {
        log.info("start test");
    }
}