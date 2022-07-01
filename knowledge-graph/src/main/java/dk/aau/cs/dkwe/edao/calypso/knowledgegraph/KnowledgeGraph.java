package dk.aau.cs.dkwe.edao.calypso.knowledgegraph;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class KnowledgeGraph implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8083);
    }

    public static void main(String[] args)
    {
        SpringApplication.run(KnowledgeGraph.class, args);
    }

    @GetMapping("test3")
    public String test3()
    {
        return "Test 3";
    }
}