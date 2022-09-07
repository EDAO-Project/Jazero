package dk.aau.cs.dkwe.edao.calypso.datalake;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.EmbeddingsFactory;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.ExplainableCause;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexWriter;
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.EmbeddingsParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.StorageHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@RestController
public class DataLake implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    private static final int THREADS = 4;
    private static final String WIKI_PREFIX = "http://www.wikipedia.org/";
    private static final String URI_PREFIX = "http://dbpedia.org/";
    private static final String KG_HOST = "http://localhost";
    private static final int KG_PORT = 8083;
    private static final String ENTITY_LINKER_HOST = "http://localhost:8082";
    private static final int ENTITY_LINKER_PORT = 8082;
    private static final File DATA_DIR = new File("./");
    private static final File INDEX_DIR = new File(".indexes/");

    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8081);
    }

    public static void main(String[] args)
    {
        SpringApplication.run(DataLake.class, args);
    }

    /**
     * Used to verify service is running
     */
    @GetMapping(value = "/ping")
    public ResponseEntity<String> ping()
    {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    /**
     * POST request to query data lake.
     * @param headers Requires Content-Type: application/json
     * @param body Query as JSON string on the form:
     *             {
     *                  "queries": [
     *                      [<TUPLE_1,1>, <TUPLE_1,2>, ..., <TUPLE_1,n>],
     *                      [<TUPLE_2,1>, <TUPLE_2,2>, ..., <TUPLE_2,n>],
     *                      ...
     *                      [<TUPLE_n,1>, <TUPLE_n,2>, ..., <TUPLE_n,n>]
     *                  ]
     *             }
     * @return JSON array of found tables
     */
    @PostMapping(value = "/search")
    public ResponseEntity<String> search(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        return ResponseEntity.ok().build();
    }

    /**
     * POST request to load data lake
     * Make sure to only use this once as it will delete any previously loaded data
     * @param headers Requires:
     *                Content-Type: application/json
     *                Storage-Type: native|HDFS
     *
     * @param body JSON string with path to directory of JSON table files. Format:
     *             {
     *                  "directory": "<DIRECTORY>"
     *             }
     *
     *             Optionally, an entry 'disallowed_types' for a JSON array of entity types can be added to indicated
     *             entity types that should be removed
     * @return
     */
    @PostMapping(value = "/insert")
    public ResponseEntity<String> insert(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String bodyKey = "directory";

        if (!INDEX_DIR.isDirectory())
        {
            INDEX_DIR.mkdir();
        }

        if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
        {
            return ResponseEntity.badRequest().body("Content-Type header must be " + MediaType.APPLICATION_JSON);
        }

        else if (!headers.containsKey("Storage-Type") ||
                !headers.get("Storage-Type").equals("native") || !headers.get("Storage-Type").equals("HDFS"))
        {
            return ResponseEntity.badRequest().body("Storage-Type header must be either 'native' or 'HDFS'");
        }

        else if (!body.containsKey(bodyKey))
        {
            return ResponseEntity.badRequest().body("Body must be a JSON string containing a single entry '" + bodyKey + "'");
        }

        File dir = new File(body.get(bodyKey));
        StorageHandler.StorageType storageType = headers.get("Storage-Type").equals("native") ?
                StorageHandler.StorageType.NATIVE : StorageHandler.StorageType.HDFS;

        if (!dir.exists() || !dir.isDirectory())
        {
            return ResponseEntity.badRequest().body("'" + dir.toString() + "' is not a directory");
        }

        try
        {
            KGService kgService = new KGService(KG_HOST, KG_PORT);
            ELService elService = new ELService(ENTITY_LINKER_HOST, ENTITY_LINKER_PORT);
            Stream<Path> fileStream = Files.find(dir.toPath(), Integer.MAX_VALUE,
                    (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> filePaths = fileStream.collect(Collectors.toList());
            Collections.sort(filePaths);
            Logger.logNewLine(Logger.Level.INFO, "There are " + filePaths.size() + " files to be processed.");

            IndexWriter indexWriter = new IndexWriter(filePaths, INDEX_DIR, DATA_DIR, storageType, kgService, elService, THREADS,
                    true, WIKI_PREFIX, URI_PREFIX);
            indexWriter.performIO();

            if (!kgService.insertLinks(DATA_DIR))
            {
                Logger.logNewLine(Logger.Level.ERROR, "Failed inserting generated TTL mapping files into KG service");
            }

            Logger.logNewLine(Logger.Level.INFO, "Elapsed time: " + indexWriter.elapsedTime() / (1e9) + " seconds\n");

            Set<Type> entityTypes = new HashSet<>();
            Iterator<Id> idIter = indexWriter.getEntityLinker().uriIds();

            while (idIter.hasNext())
            {
                Entity entity = indexWriter.getEntityTable().find(idIter.next());

                if (entity != null)
                    entityTypes.addAll(entity.getTypes());
            }

            Logger.logNewLine(Logger.Level.INFO, "Found an approximate total of " + indexWriter.getApproximateEntityMentions() + " unique entity mentions across " + indexWriter.cellsWithLinks() + " cells \n");
            Logger.logNewLine(Logger.Level.INFO, "There are in total " + entityTypes.size() + " unique entity types across all discovered entities.");
            Logger.logNewLine(Logger.Level.INFO, "Indexing took " + indexWriter.elapsedTime() + " ns");

            return ResponseEntity.ok("Loaded tables: " + indexWriter.loadedTables() + "\nElapsed time: " + indexWriter.elapsedTime() + "ns");
        }

        catch (IOException e)
        {
            return ResponseEntity.badRequest().body("Error locating JSON table files: " + e.getMessage());
        }
    }

    /**
     * POST request to load embeddings
     * In the embeddings file, each entity and embedding must be separated by new line
     * In each line, start with the entity URI and follow by its embedding values
     * Use the same delimiter to separate embedding values and entity URI from its embedding
     * @param headers Requires:
     *                Content-Type: application/json
     * @param body Requires JSON with two entries:
     *             {
     *                  "file": "<PATH/TO/EMBEDDINGS>"
     *                  "delimiter: " "<Delimiter separating each floating-point value and entity string>"
     *             }
     * @return
     */
    @PostMapping(value = "/embeddings")
    public ResponseEntity<String> loadEmbeddings(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
        {
            return ResponseEntity.badRequest().body("Content-Type header must be " + MediaType.APPLICATION_JSON);
        }

        else if (!body.containsKey("file"))
        {
            return ResponseEntity.badRequest().body("Missing 'file' entry in JSON body of POST request");
        }

        else if (!body.containsKey("delimiter"))
        {
            return ResponseEntity.badRequest().body("Delimiter character has not been specified for embeddings file");
        }

        Configuration.setDBHost("127.0.0.1");
        Configuration.setDBPort(5432);

        try
        {
            EmbeddingsParser parser = new EmbeddingsParser(new FileInputStream(body.get("file")), body.get("delimiter").charAt(0));
            DBDriverBatch<List<Double>, String> db = EmbeddingsFactory.fromConfig(true);
            int batchSize = 100, batchSizeCount = batchSize;
            double loaded = 0;

            while (parser.hasNext())
            {
                int bytes = insertEmbeddings(db, parser, batchSize);
                loaded += (double) bytes / Math.pow(1024, 2);

                if (bytes == 0)
                    Logger.logNewLine(Logger.Level.ERROR, "INSERTION ERROR: " + ((ExplainableCause) db).getError());

                else
                    Logger.log(Logger.Level.INFO, "LOAD BATCH [" + batchSizeCount + "] - " + loaded + " mb");

                batchSizeCount += bytes > 0 ? batchSize : 0;
            }

            db.close();
            return ResponseEntity.ok("Loaded " + batchSizeCount + " entity embeddings (" + loaded + " mb)");
        }

        catch (FileNotFoundException e)
        {
            return ResponseEntity.badRequest().body("Embeddings file does not exist");
        }
    }

    private static int insertEmbeddings(DBDriverBatch<?, ?> db, EmbeddingsParser parser, int batchSize)
    {
        String entity = null;
        List<List<Float>> vectors = new ArrayList<>(batchSize);
        List<Float> embedding = new ArrayList<>();
        List<String> iris = new ArrayList<>(batchSize);
        int count = 0, loaded = 0;
        EmbeddingsParser.EmbeddingToken prev = parser.prev(), token;

        if (prev != null && prev.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
        {
            entity = prev.getLexeme();
            iris.add(entity);
            count++;
            loaded = entity.length() + 1;
        }

        while (parser.hasNext() && count < batchSize && (token = parser.next()) != null)
        {
            if (token.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
            {
                if (entity != null)
                    vectors.add(new ArrayList<>(embedding));

                entity = token.getLexeme();
                iris.add(entity);
                embedding.clear();
                count++;
                loaded += entity.length() + 1;
            }

            else
            {
                String lexeme = token.getLexeme();
                embedding.add(Float.parseFloat(lexeme));
                loaded += lexeme.length() + 1;
            }
        }

        if (!iris.isEmpty())
            iris.remove(iris.size() - 1);

        return db.batchInsert(iris, vectors) ? loaded : 0;
    }
}