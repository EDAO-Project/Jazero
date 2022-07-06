package dk.aau.cs.dkwe.edao.calypso.datalake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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

    @RequestMapping(value = "/search", method = RequestMethod.GET)
    public String search()
    {
        return "search";
    }

    @RequestMapping(value = "/insert", method = RequestMethod.POST)
    public String insert()
    {
        return "insert";
    }

    /**
     * Used as handshake and to verify service is running
     */
    @RequestMapping(value = "/ping", method = RequestMethod.GET)
    public String ping()
    {
        return "Pong";
    }
}