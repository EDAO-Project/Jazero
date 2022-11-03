package dk.aau.cs.dkwe.edao.calypso.entitylinker;

import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.calypso.entitylinker.index.LuceneFactory;
import dk.aau.cs.dkwe.edao.calypso.entitylinker.link.LuceneLinker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

@SpringBootApplication
@RestController
public class EntityLinker implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8082);
    }

    public static void main(String[] args) throws IOException
    {
        if (!LuceneFactory.isBuild())
        {
            Logger.logNewLine(Logger.Level.INFO, "No Lucene index found");

            while (true)
            {
                try
                {
                    KGService kgService = new KGService(Configuration.getEKGManagerHost(), Configuration.getEKGManagerPort());
                    Map<String, Set<String>> subGraph = kgService.getSubGraph();
                    LuceneFactory.build(subGraph, true);
                    Logger.logNewLine(Logger.Level.INFO, "Lucene index build finished");
                    break;
                }

                catch (RuntimeException e) {}
            }
        }

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

        try
        {
            LuceneLinker luceneLinker = new LuceneLinker(LuceneFactory.get());
            String linkedEntity = luceneLinker.link(input);
            return ResponseEntity.ok(linkedEntity != null ? linkedEntity : "None");
        }

        catch (IOException e)
        {
            throw new RuntimeException("Could not load Lucene index from disk: " + e.getMessage());
        }
    }
}