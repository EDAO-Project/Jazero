package dk.aau.cs.dkwe.edao.calypso.entitylinker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class EntityLinker implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8082);
    }

    public static void main(String[] args)
    {
        SpringApplication.run(EntityLinker.class, args);
    }

    @GetMapping("/test2")
    public String test2()
    {
        return "Test 2";
    }
}