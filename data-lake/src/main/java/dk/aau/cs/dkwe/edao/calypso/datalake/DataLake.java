package dk.aau.cs.dkwe.edao.calypso.datalake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping(value = "/search")
    public String search(@RequestParam(value = "jsonQuery", defaultValue = "{}") String query)
    {
        return query;
    }

    @PostMapping(value = "/insert")
    public String insert()
    {
        return "insert";
    }

    /**
     * Used as handshake and to verify service is running
     */
    @GetMapping(value = "/ping")
    public String ping()
    {
        return "pong";
    }
}