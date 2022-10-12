package dk.aau.cs.dkwe.edao.calypso.entitylinker;

import dk.aau.cs.dkwe.edao.calypso.entitylinker.link.GoogleKGEntityLinker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

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

    @GetMapping("/ping")
    public ResponseEntity<String> ping()
    {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    /**
     * Entry for linking input entity to KG entity
     * @param headers Requires:
     *                Content-Type: application/json
     * @param body This is a JSON body on the following format with one entry:
     *             {
     *                 "input": "<INPUT ENTITY>"
     *             }
     * @return String of KG entity node
     */
    @PostMapping("/link")
    public ResponseEntity<String> link(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!body.containsKey("input"))
        {
            return ResponseEntity.badRequest().body("Missing 'input' entry in body specifying entity to be linked to the EKG");
        }

        String input = body.get("input");

        if (input.contains("http"))
        {
            String[] split = input.split("/");
            input = split[split.length - 1].replace('_', ' ');
        }

        String linkedEntity = GoogleKGEntityLinker.make().link(input);
        return ResponseEntity.ok(linkedEntity != null ? linkedEntity : "None");
    }
}