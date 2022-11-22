package dk.aau.cs.dkwe.edao.calypso.entitylinker;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.TableDeserializer;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.TableSerializer;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.calypso.datalake.utilities.Deserializer;
import dk.aau.cs.dkwe.edao.calypso.entitylinker.index.LuceneFactory;
import dk.aau.cs.dkwe.edao.calypso.entitylinker.link.LuceneLinker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
public class EntityLinker implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    private static LuceneLinker luceneLinker;

    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8082);
    }

    public static void main(String[] args) throws IOException
    {
        if (!LuceneFactory.isBuild())
        {
            Logger.log(Logger.Level.INFO, "No Lucene index found");

            File kgDir = new File(Configuration.getKGDir());
            LuceneFactory.build(kgDir, true);
            Logger.log(Logger.Level.INFO, "Lucene index build finished");
        }

        luceneLinker = new LuceneLinker(LuceneFactory.get());
        SpringApplication.run(EntityLinker.class, args);
    }

    @GetMapping("/ping")
    public ResponseEntity<String> ping()
    {
        Logger.log(Logger.Level.INFO, "PING");
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    private static String linkInput(String input)
    {
        return luceneLinker.link(input);
    }

    private static String urlToStr(String url)
    {
        String[] split = url.split("/");

        if (split.length == 0)
        {
            return url.replace('_', ' ');
        }

        return split[split.length - 1].replace('_', ' ');
    }

    // '&'-separated string of entities in given list of entities
    private static String serialize(List<String> entities)
    {
        StringBuilder builder = new StringBuilder();

        for (String entity : entities)
        {
            builder.append(entity).append("&");
        }

        return builder.deleteCharAt(builder.length() - 1).toString();
    }

    private static List<String> deserialize(String serialized)
    {
        String[] split = serialized.split("&");
        List<String> entities = new ArrayList<>(split.length);

        for (String entity : split)
        {
            entities.add(entity);
        }

        return entities;
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
            input = urlToStr(input);
        }

        String linkedEntity = linkInput(input);
        return ResponseEntity.ok(linkedEntity != null ? linkedEntity : "None");
    }

    /**
     * Links all entities in a table
     * @param headers Requires:
     *                Content-Type: application/json
     * @param body Must contain JSON entry "table" as seen below:
     *             {
     *                 "table": "<SERIALIZED TABLE (use TableSerializer)>"
     *             }
     * @return Serialized table according to TableSerializer
     */
    @PostMapping("/link-table")
    public ResponseEntity<String> linkTable(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        String key = "table";

        if (!body.containsKey(key))
        {
            return ResponseEntity.badRequest().body("Missing '" + key + "' entry in body specifying table of entities to be linked to the EKG");
        }

        Table<String> table = TableDeserializer.create(body.get(key)).deserialize();
        Table<String> linkedTable = new DynamicTable<>();
        int rows = table.rowCount();

        for (int row = 0; row < rows; row++)
        {
            int columns = table.getRow(row).size();
            List<String> tableRow = new ArrayList<>(columns);

            for (int column = 0; column < columns; column++)
            {
                String input = table.getRow(row).get(column);

                if (input.contains("http"))
                {
                    input = urlToStr(input);
                }

                String linked = linkInput(input);
                tableRow.add(linked != null ? linked : "None");
            }

            linkedTable.addRow(new Table.Row<>(tableRow));
        }

        return ResponseEntity.ok(TableSerializer.create(linkedTable).serialize());
    }
}