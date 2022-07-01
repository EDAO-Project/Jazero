package dk.aau.cs.dkwe.edao.calypso.datalake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class DataLake implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8081);
    }

    public static void main(String[] args)
    {
        SpringApplication.run(DataLake.class, args);
    }

    @GetMapping("/search")
    public String search()
    {
        return "search";
    }

    @GetMapping("/insert")
    public String insert()
    {
        return "insert";
    }
}