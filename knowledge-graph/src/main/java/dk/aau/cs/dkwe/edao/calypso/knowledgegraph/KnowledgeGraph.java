package dk.aau.cs.dkwe.edao.calypso.knowledgegraph;

import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware.Neo4JHandler;
import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware.Neo4JReader;
import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware.Neo4JWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

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
        if (!Neo4JHandler.isInstalled())
        {
            throw new RuntimeException("Neo4J has not been installed. Follow README instruction how to do so");
        }

        SpringApplication.run(KnowledgeGraph.class, args);
    }

    @GetMapping("/ping")
    public ResponseEntity<String> ping()
    {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    /**
     * Request a serialized view of the full KG
     * the KG is returned in a file
     * @return Full KG
     */
    @GetMapping("/get-kg")
    public synchronized ResponseEntity<String> getKG(@RequestHeader Map<String, String> headers)
    {
        if (!Neo4JHandler.isInstalled())
        {
            return ResponseEntity.internalServerError().body("Neo4J is not installed");
        }

        try
        {
            Neo4JReader reader = new Neo4JReader();
            reader.performIO();

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body("{\"file\": " + reader.getGraphFile() + "}");
        }

        catch (IOException exc)
        {
            return ResponseEntity.internalServerError().body("Failed reading KG: " + exc.getMessage());
        }
    }

    /**
     * Inserts KG file into a Neo4J instance
     * Header must contain entry Content-Type: application/json
     * Body must be JSON and contain an entry "file": "<KG FILE>"
     */
    @PostMapping("/set-kg")
    public synchronized ResponseEntity<String> setKG(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!Neo4JHandler.isInstalled())
        {
            return ResponseEntity.internalServerError().body("Neo4J is not installed");
        }

        else if (!checkHeaders(headers))
        {
            return ResponseEntity.badRequest().body("Content-Type must be " + MediaType.APPLICATION_JSON);
        }

        else if (!body.containsKey("file"))
        {
            return ResponseEntity.badRequest().body("Missing 'file' entry in body that specifies KG file");
        }

        try
        {
            String file = body.get("file");
            Neo4JWriter writer = new Neo4JWriter(file);
            writer.performIO();

            return ResponseEntity.ok().build();
        }

        catch (IOException exc)
        {
            return ResponseEntity.internalServerError().body("Failed inserting KG: " + exc.getMessage());
        }
    }

    /**
     * Inserts table node linked to all passed entity nodes.
     * This is given in a folder of Turtle files
     * JSON format:
     *  {
     *      "folder": "<FOLDER>"
     *  }
     *
     *  The folder must have two .ttl files: one with table ID to entities and one with table ID to types
     *  Table ID to entities must contain lines of the format <TABLE ID URI> <https://schema.org/mentions> <KG entity IRI>
     *  Table ID to types must contain lines of the format <TABLE ID URI> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Table>
     */
    @PostMapping("/insert-links")
    public synchronized ResponseEntity<String> insertLinks(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!Neo4JHandler.isInstalled())
        {
            return ResponseEntity.internalServerError().body("Neo4J is not installed");
        }

        else if (!checkHeaders(headers))
        {
            return ResponseEntity.badRequest().body("Content-Type must be " + MediaType.APPLICATION_JSON);
        }

        else if (!body.containsKey("folder"))
        {
            return ResponseEntity.badRequest().body("Missing 'folder' entry in body that specifies folder of table links");
        }

        try
        {
            Neo4JWriter.insertTableToEntities(body.get("folder"));
            return ResponseEntity.ok().build();
        }

        catch (IOException e)
        {
            return ResponseEntity.internalServerError().body("Failed inserting table to entity links: " + e.getMessage());
        }
    }

    private static boolean checkHeaders(Map<String, String> headers)
    {
        return !headers.containsKey("Content-Type") || !headers.get("Content-Type").equals(MediaType.APPLICATION_JSON);
    }
}