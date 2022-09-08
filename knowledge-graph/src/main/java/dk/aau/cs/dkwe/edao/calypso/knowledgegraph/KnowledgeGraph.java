package dk.aau.cs.dkwe.edao.calypso.knowledgegraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
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
import java.util.List;
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

        boolean ret = Neo4JHandler.start();

        if (!ret)
        {
            throw new RuntimeException("Failed to start Neo4J graph database");
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

        catch (IOException | RuntimeException exc)
        {
            return ResponseEntity.internalServerError().body("Failed reading KG: " + exc.getMessage());
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

        else if (!headers.containsKey("content-type") || !headers.get("Content-Type").equals(MediaType.APPLICATION_JSON_VALUE))
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

    /**
     * Return JSON array of types of given entity
     * Body must contain a single JSON entry with entity URI:
     *      {
     *          "entity": "<URI>"
     *      }
     * @return JSON array of entity types of format {"types": ["<TYPE_1>", "<TYPE_2>", ..., "<TYPE_N>"]}
     */
    @PostMapping("/types")
    public synchronized ResponseEntity<String> entityTypes(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String entry = "entity";

        if (!Neo4JHandler.isInstalled())
        {
            return ResponseEntity.internalServerError().body("Neo4J is not installed");
        }

        else if (!headers.containsKey(entry))
        {
            return ResponseEntity.badRequest().body("Missing entry \"Entity\" to specify entity to find types of");
        }

        try
        {
            Neo4JReader reader = new Neo4JReader();
            List<Type> types = reader.entityTypes(new Entity(headers.get(entry)));

            if (types == null)
            {
                return ResponseEntity.badRequest().body("Entity '" + headers.get(entry) + "' does not exist in KG");
            }

            JsonObject object = new JsonObject();
            JsonArray array = new JsonArray(types.size());
            types.forEach(t -> array.add(t.getType()));
            object.add("types", array);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(object.toString());
        }

        catch (RuntimeException e)
        {
            return ResponseEntity.internalServerError().body("Neo4J error: " + e.getMessage());
        }
    }
}