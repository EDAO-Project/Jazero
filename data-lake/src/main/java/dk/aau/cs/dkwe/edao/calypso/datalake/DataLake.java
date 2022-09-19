package dk.aau.cs.dkwe.edao.calypso.datalake;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.EmbeddingsFactory;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.ExplainableCause;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexReader;
import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexWriter;
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.EmbeddingsParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.search.Result;
import dk.aau.cs.dkwe.edao.calypso.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@RestController
public class DataLake implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    private static EntityLinking linker;
    private static EntityTable entityTable;
    private static EntityTableLink tableLink;
    private static final int THREADS = 4;
    private static final String WIKI_PREFIX = "http://www.wikipedia.org/";
    private static final String URI_PREFIX = "http://dbpedia.org/";
    private static final String KG_HOST = "127.0.0.1";
    private static final int KG_PORT = 8083;
    private static final String ENTITY_LINKER_HOST = "127.0.0.1";
    private static final int ENTITY_LINKER_PORT = 8082;
    private static final File DATA_DIR = new File("../knowledge-graph/neo4j/mappings/");
    private static final File INDEX_DIR = new File(".indexes/");

    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(8081);
    }

    public static void main(String[] args)
    {
        if (Configuration.areIndexesLoaded())
        {
            try
            {
                IndexReader indexReader = new IndexReader(INDEX_DIR, true, true);
                indexReader.performIO();

                linker = indexReader.getLinker();
                entityTable = indexReader.getEntityTable();
                tableLink = indexReader.getEntityTableLink();
            }

            catch (IOException e)
            {
                throw new RuntimeException("IOException when reading indexes: " + e.getMessage());
            }
        }

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
     * @param headers Requires:
     *                Content-Type: application/json
     *                Top-K: <K VALUE>
     * @param body Query as JSON string on the form:
     *             {
     *                  "top-k": "<INTEGER VALUE>",
     *                  "use-embeddings": "<BOOLEAN VALUE>",
     *                  "single-column-per-query-entity": "<BOOLEAN VALUE>",
     *                  "weighted-jaccard": "<BOOLEAN VALUE>,
     *                  "adjusted-jaccard": "<BOOLEAN VALUE>",
     *                  "use-max-similarity-per-column": "<BOOLEAN VALUE>",
     *                  "query": [
     *                      [<TUPLE_1,1>, <TUPLE_1,2>, ..., <TUPLE_1,n>],
     *                      [<TUPLE_2,1>, <TUPLE_2,2>, ..., <TUPLE_2,n>],
     *                      ...
     *                      [<TUPLE_n,1>, <TUPLE_n,2>, ..., <TUPLE_n,n>]
     *                  ]
     *             }
     * @return JSON array of found tables. Each element is a pair of table ID and score.
     */
    @PostMapping(value = "/search")
    public ResponseEntity<String> search(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!Configuration.areIndexesLoaded())
        {
            return ResponseEntity.badRequest().body("Indexes have not been loaded. Use the '/insert' endpoint.");
        }

        else if (!Configuration.areEmbeddingsLoaded())
        {
            return ResponseEntity.badRequest().body("Embeddings have not been loaded. Use the '/embeddings' endpoint.");
        }

        else if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
        {
            return ResponseEntity.badRequest().body("Content-Type header must be " + MediaType.APPLICATION_JSON);
        }

        else if (!body.containsKey("query"))
        {
            return ResponseEntity.badRequest().body("Missing 'query' in JSON body");
        }

        else if (!body.containsKey("top-k"))
        {
            return ResponseEntity.badRequest().body("Missing 'Top-K' in JSON body");
        }

        else if (!body.containsKey("use-embeddings"))
        {
            return ResponseEntity.badRequest().body("Missing 'use-embeddings' in JSON body");
        }

        else if (!body.containsKey("single-column-per-query-entity"))
        {
            return ResponseEntity.badRequest().body("Missing 'single-column-per-query-entity' in JSON body");
        }

        else if (!body.containsKey("weighted-jaccard"))
        {
            return ResponseEntity.badRequest().body("Missing 'weighted-jaccard' in JSON body");
        }

        else if (!body.containsKey("adjusted-jaccard"))
        {
            return ResponseEntity.badRequest().body("Missing 'adjusted-jaccard' in JSON body");
        }

        else if (!body.containsKey("use-max-similarity-per-column"))
        {
            return ResponseEntity.badRequest().body("Missing 'use-max-similarity-per-column' in JSON body");
        }

        int topK = Integer.parseInt(body.get("top-k"));
        boolean useEmbeddings = Boolean.parseBoolean(body.get("use-embeddings"));
        boolean singleColumnPerEntity = Boolean.parseBoolean(body.get("single-column-per-query-entity"));
        boolean weightedJaccard = Boolean.parseBoolean(body.get("weighted-jaccard"));
        boolean adjustedJaccard = Boolean.parseBoolean(body.get("adjusted-jaccard"));
        boolean useMaxSimilarityPerColumn = Boolean.parseBoolean(body.get("use-max-similarity-per-column"));
        StorageHandler storageHandler = new StorageHandler(Configuration.getStorageType());
        DBDriverBatch<List<Double>, String> embeddingsDB = EmbeddingsFactory.fromConfig(false);
        TableSearch search = new TableSearch(storageHandler, linker, entityTable, tableLink, topK, THREADS, useEmbeddings,
                null, singleColumnPerEntity, weightedJaccard, adjustedJaccard, useMaxSimilarityPerColumn, false,
                null, embeddingsDB);
        Result result = search.search(null);
        embeddingsDB.close();

        JsonObject object = new JsonObject();
        JsonArray array = new JsonArray(result.getK());
        Iterator<Pair<File, Double>> scores = result.getResults();

        while (scores.hasNext())
        {
            Pair<File, Double> score = scores.next();
            JsonObject jsonScore = new JsonObject();
            jsonScore.add("table", new JsonPrimitive(score.getFirst().getName()));
            jsonScore.add("score", new JsonPrimitive(score.getSecond()));
            array.add(jsonScore);
        }

        object.add("scores", array);
        return ResponseEntity
                .ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(object.toString());
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

        if (!DATA_DIR.mkdir())
        {
            DATA_DIR.mkdir();
        }

        if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
        {
            return ResponseEntity.badRequest().body("Content-Type header must be " + MediaType.APPLICATION_JSON);
        }

        else if (!headers.containsKey("storage-type") ||
                (!headers.get("storage-type").equals("native") && !headers.get("storage-type").equals("HDFS")))
        {
            return ResponseEntity.badRequest().body("Storage-Type header must be either 'native' or 'HDFS'");
        }

        else if (!body.containsKey(bodyKey))
        {
            return ResponseEntity.badRequest().body("Body must be a JSON string containing a single entry '" + bodyKey + "'");
        }

        File dir = new File(body.get(bodyKey));
        StorageHandler.StorageType storageType = headers.get("storage-type").equals("native") ?
                StorageHandler.StorageType.NATIVE : StorageHandler.StorageType.HDFS;

        if (!dir.exists() || !dir.isDirectory())
        {
            return ResponseEntity.badRequest().body("'" + dir + "' is not a directory");
        }

        try
        {
            long totalTime = System.nanoTime();
            KGService kgService = new KGService(KG_HOST, KG_PORT);
            ELService elService = new ELService(ENTITY_LINKER_HOST, ENTITY_LINKER_PORT);

            if (kgService.size() < 1)
            {
                Logger.logNewLine(Logger.Level.ERROR, "KG is empty. Make sure to load the KG according to README. Continuing...");
            }

            Stream<Path> fileStream = Files.find(dir.toPath(), Integer.MAX_VALUE,
                    (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> filePaths = fileStream.collect(Collectors.toList());
            Collections.sort(filePaths);
            Logger.logNewLine(Logger.Level.INFO, "There are " + filePaths.size() + " files to be processed.");

            IndexWriter indexWriter = new IndexWriter(filePaths, INDEX_DIR, DATA_DIR, storageType, kgService, elService, THREADS,
                    WIKI_PREFIX, URI_PREFIX);
            indexWriter.performIO();

            if (!kgService.insertLinks(DATA_DIR))
            {
                Logger.logNewLine(Logger.Level.ERROR, "Failed inserting generated TTL mapping files into KG service");
            }

            Set<Type> entityTypes = new HashSet<>();
            Iterator<Id> idIter = indexWriter.getEntityLinker().uriIds();

            while (idIter.hasNext())
            {
                Entity entity = indexWriter.getEntityTable().find(idIter.next());

                if (entity != null)
                    entityTypes.addAll(entity.getTypes());
            }

            totalTime = System.nanoTime() - totalTime;
            Logger.logNewLine(Logger.Level.INFO, "Found an approximate total of " + indexWriter.getApproximateEntityMentions() + " unique entity mentions across " + indexWriter.cellsWithLinks() + " cells \n");
            Logger.logNewLine(Logger.Level.INFO, "There are in total " + entityTypes.size() + " unique entity types across all discovered entities.");
            Logger.logNewLine(Logger.Level.INFO, "Indexing took " +
                    TimeUnit.SECONDS.convert(indexWriter.elapsedTime(), TimeUnit.NANOSECONDS) + "s");
            Configuration.setIndexesLoaded(true);

            return ResponseEntity.ok("Loaded tables: " + indexWriter.loadedTables() + "\nIndex time: " +
                    TimeUnit.SECONDS.convert(indexWriter.elapsedTime(), TimeUnit.NANOSECONDS) + "s\nTotal elapsed time: " +
                    TimeUnit.SECONDS.convert(totalTime, TimeUnit.NANOSECONDS));
        }

        catch (IOException e)
        {
            Configuration.setIndexesLoaded(false);
            return ResponseEntity.badRequest().body("Error locating JSON table files: " + e.getMessage());
        }

        catch (RuntimeException e)
        {
            Configuration.setIndexesLoaded(false);
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
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

            Configuration.setEmbeddingsLoaded(true);
            db.close();

            return ResponseEntity.ok("Loaded " + batchSizeCount + " entity embeddings (" + loaded + " mb)");
        }

        catch (FileNotFoundException e)
        {
            Configuration.setEmbeddingsLoaded(false);
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