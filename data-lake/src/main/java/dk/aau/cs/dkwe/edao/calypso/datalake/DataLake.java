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
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.ParsingException;
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.search.Prefilter;
import dk.aau.cs.dkwe.edao.calypso.datalake.search.Result;
import dk.aau.cs.dkwe.edao.calypso.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EmbeddingsIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.SetLSHIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.VectorLSHIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.EndpointAnalysis;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.FileLogger;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.StorageHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.*;
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
    private static EmbeddingsIndex<Id> embeddingsIndex;
    private static SetLSHIndex typesLSH;
    private static VectorLSHIndex embeddingLSH;
    private static EndpointAnalysis analysis;
    private static final int THREADS = 4;
    private static final File DATA_DIR = new File("/index/mappings/");
    private boolean indexLoadingInProgress = false, embeddingsLoadingInProgress = false;

    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(Configuration.getSDLManagerPort());
    }

    public static void main(String[] args)
    {
        FileLogger.log(FileLogger.Service.SDL_Manager, "Starting...");
        loadIndexes();

        FileLogger.log(FileLogger.Service.SDL_Manager, "Started");
        FileLogger.log(FileLogger.Service.SDL_Manager, "SDL Manager available at 0.0.0.0:" + Configuration.getSDLManagerPort());
        SpringApplication.run(DataLake.class, args);
    }

    private static void loadIndexes()
    {
        analysis = new EndpointAnalysis();

        if (Configuration.areIndexesLoaded())
        {
            try
            {
                IndexReader indexReader = new IndexReader(new File(Configuration.getIndexDir()), true, true);
                indexReader.performIO();

                linker = indexReader.getLinker();
                entityTable = indexReader.getEntityTable();
                tableLink = indexReader.getEntityTableLink();
                embeddingsIndex = indexReader.getEmbeddingsIndex();
                typesLSH = indexReader.getTypesLSH();
                embeddingLSH = indexReader.getVectorsLSH();

                typesLSH.useEntityLinker(linker);
                typesLSH.useEntityTable(entityTable);
                embeddingLSH.useEntityLinker(linker);
                embeddingLSH.useEmbeddingIndex(embeddingsIndex);
            }

            catch (IOException | RuntimeException e)
            {
                FileLogger.log(FileLogger.Service.SDL_Manager, "Failed loading indexes: " + e.getMessage());
                Configuration.setIndexesLoaded(false);
            }
        }
    }

    /**
     * Used to verify service is running
     */
    @GetMapping(value = "/ping")
    public ResponseEntity<String> ping()
    {
        analysis.record("ping", 1);
        Logger.log(Logger.Level.INFO, "PING");
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    /**
     * POST request to query data lake.
     * @param headers Requires:
     *                Content-Type: application/json
     * @param body Query as JSON string on the form:
     *             {
     *                  "top-k": "<INTEGER VALUE>",
     *                  "entity-similarity": "TYPES|PREDICATES|EMBEDDINGS"
     *                  "single-column-per-query-entity": "<BOOLEAN VALUE>",
     *                  "use-max-similarity-per-column": "<BOOLEAN VALUE>",
     *                  ["weighted-jaccard": "<BOOLEAN VALUE>,]
     *                  ["cosine-function": "NORM_COS|ABS_COS|ANG_COS"]
     *                  ["lsh": "TYPES|EMBEDDINGS",]
     *                  "query": "<QUERY STRING>"
     *             }
     * <p>
     *             The <QUERY STRING> is a list of tuples, each tuple separated by a hashtag (#),
     *             and each tuple element is separated by a diamond (<>).
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

        else if (!body.containsKey("entity-similarity"))
        {
            return ResponseEntity.badRequest().body("Missing 'entity-similarity' in JSON body");
        }

        else if (!body.containsKey("single-column-per-query-entity"))
        {
            return ResponseEntity.badRequest().body("Missing 'single-column-per-query-entity' in JSON body");
        }

        else if (!body.containsKey("use-max-similarity-per-column"))
        {
            return ResponseEntity.badRequest().body("Missing 'use-max-similarity-per-column' in JSON body");
        }

        else if (!body.containsKey("similarity-measure"))
        {
            return ResponseEntity.badRequest().body("Missing 'similarity-measure' in JSON body");
        }

        int topK = Integer.parseInt(body.get("top-k"));
        boolean singleColumnPerEntity = Boolean.parseBoolean(body.get("single-column-per-query-entity"));
        boolean useMaxSimilarityPerColumn = Boolean.parseBoolean(body.get("use-max-similarity-per-column"));
        boolean weightedJaccard = false;
        String entitySimStr = body.get("entity-similarity");
        TableSearch.SimilarityMeasure similarityMeasure = TableSearch.SimilarityMeasure.valueOf(body.get("similarity-measure"));
        Prefilter prefilter = null;
        TableSearch.EntitySimilarity entitySimilarity;

        if (entitySimStr.equals("EMBEDDINGS"))
        {
            if (!body.containsKey("cosine-function"))
            {
                return ResponseEntity.badRequest().body("Missing cosine similarity function when searching using embeddings");
            }

            String cosFunction = body.get("cosine-function");

            if (cosFunction.contains("NORM_COS"))
            {
                entitySimilarity = TableSearch.EntitySimilarity.EMBEDDINGS_NORM;
            }

            else if (cosFunction.contains("ABS_COS"))
            {
                entitySimilarity = TableSearch.EntitySimilarity.EMBEDDINGS_ABS;
            }

            else if (cosFunction.contains("ANG_COS"))
            {
                entitySimilarity = TableSearch.EntitySimilarity.EMBEDDINGS_ANG;
            }

            else
            {
                return ResponseEntity.badRequest().body("Did not understand cosine function ' " + cosFunction + "'");
            }
        }

        else
        {
            if (!body.containsKey("weighted-jaccard"))
            {
                return ResponseEntity.badRequest().body("Missing 'weighted-jaccard' when searching using entity types or predicates");
            }

            weightedJaccard = Boolean.parseBoolean(body.get("weighted-jaccard"));
            entitySimilarity = entitySimStr.equals("TYPES") ? TableSearch.EntitySimilarity.JACCARD_TYPES
                    : entitySimStr.equals("PREDICATES") ? TableSearch.EntitySimilarity.JACCARD_PREDICATES : null;

            if (entitySimilarity == null)
            {
                return ResponseEntity.badRequest().body("Did not understand entity similarity metric '" + entitySimStr + "'");
            }
        }

        if (body.containsKey("lsh"))
        {
            String lshType = body.get("lsh").toUpperCase();

            if (lshType.equals("TYPES"))
            {
                prefilter = new Prefilter(linker, entityTable, tableLink, embeddingsIndex, typesLSH);
            }

            else if (lshType.equals("EMBEDDINGS"))
            {
                prefilter = new Prefilter(linker, entityTable, tableLink, embeddingsIndex, embeddingLSH);
            }
        }

        Table<String> query = parseQuery(body.get("query"));
        Iterator<String> queryIterator = query.iterator();
        StorageHandler storageHandler = new StorageHandler(Configuration.getStorageType());
        KGService kgService = new KGService(Configuration.getEKGManagerHost(), Configuration.getEKGManagerPort());
        DBDriverBatch<List<Double>, String> embeddingsDB = EmbeddingsFactory.fromConfig(false);
        TableSearch search;

        while (queryIterator.hasNext())
        {
            String entity = queryIterator.next();
            IndexWriter.indexKGEntity(entity, linker, entityTable, kgService, embeddingsIndex, embeddingsDB);
        }

        if (prefilter != null)
        {
            search = new TableSearch(storageHandler, linker, entityTable, tableLink, embeddingsIndex, topK, THREADS,
                    entitySimilarity, singleColumnPerEntity, weightedJaccard, useMaxSimilarityPerColumn,
                    false, similarityMeasure, prefilter);
        }

        else
        {
            search = new TableSearch(storageHandler, linker, entityTable, tableLink, embeddingsIndex, topK, THREADS,
                    entitySimilarity, singleColumnPerEntity, weightedJaccard, useMaxSimilarityPerColumn,
                    false, similarityMeasure);
        }

        Result result = search.search(query);

        if (result == null)
        {
            return ResponseEntity.internalServerError().body("Internal error when searching");
        }

        JsonObject jsonResult = resultToJson(result, search.elapsedNanoSeconds(), search.getReduction());
        analysis.record("search", 1);

        return ResponseEntity
                .ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonResult.toString());
    }

    private static Table<String> parseQuery(String queryStr)
    {
        Table<String> query = new DynamicTable<>();
        String[] queryStrTuples = queryStr.split("#");

        for (String tuple : queryStrTuples)
        {
            String[] entities = tuple.split("<>");
            query.addRow(new Table.Row<>(entities));
        }

        return query;
    }

    private static JsonObject resultToJson(Result res, long runtime, double reduction)
    {
        JsonObject object = new JsonObject();
        JsonArray array = new JsonArray(res.getK());
        Iterator<Pair<File, Double>> scores = res.getResults();

        while (scores.hasNext())
        {
            Pair<File, Double> score = scores.next();
            JsonObject jsonScore = new JsonObject();
            jsonScore.add("table ID", new JsonPrimitive(score.first().getName()));

            try
            {
                Table<String> table = TableParser.parse(score.first());
                int rowCount = table.rowCount();
                JsonArray rows = new JsonArray(rowCount);

                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
                {
                    Table.Row<String> row = table.getRow(rowIdx);
                    JsonArray column = new JsonArray(row.size());
                    row.forEach(cell -> {
                        JsonObject cellObject = new JsonObject();
                        cellObject.addProperty("text", cell);
                        column.add(cellObject);
                    });

                    rows.add(column);
                }

                jsonScore.add("table", rows);
                jsonScore.add("score", new JsonPrimitive(score.second()));
                array.add(jsonScore);
            }

            catch (ParsingException e) {}
        }

        object.add("scores", array);
        object.addProperty("runtime", runtime);
        object.addProperty("reduction", reduction * 100);

        return object;
    }

    /**
     * POST request to load data lake
     * Make sure to only use this once as it will delete any previously loaded data
     * 'Signature-Size' is number of permutation/projection vectors to be used in LSH index
     * 'Band-Size' is size of bands for the LSH signature
     * @param headers Requires:
     *                Content-Type: application/json
     *                Storage-Type: native|HDFS
     *                Signature-Size: <INTEGER>
     *                Band-Size: <INTEGER>
     *
     * @param body JSON string with path to directory of JSON table files. Format:
     *             {
     *                  "directory": "<DIRECTORY>",
     *                  "table-prefix": "<TABLE PREFIX>",
     *                  "kg-prefix": "<KG PREFIX>"
     *             }
     * <p>
     *             "table-prefix" is the common prefix for table entities, just like kg-prefix. If the prefix differs
     *             from entity to entity, use the empty string.
     *             Optionally, an entry 'disallowed_types' for a JSON array of entity types can be added to indicated
     *             entity types that should be removed
     * @return Simple index build stats
     */
    @PostMapping(value = "/insert")
    public ResponseEntity<String> insert(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String dirKey = "directory", tablePrefixKey = "table-prefix", kgPrefixKey = "kg-prefix";
        File indexDir = new File(Configuration.getKGDir());

        if (!indexDir.isDirectory())
        {
            indexDir.mkdir();
        }

        if (!DATA_DIR.isDirectory())
        {
            DATA_DIR.mkdir();
        }

        if (!Configuration.areEmbeddingsLoaded())
        {
            return ResponseEntity.badRequest().body("You need to load embeddings first using the '/embeddings' endpoint");
        }

        else if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
        {
            return ResponseEntity.badRequest().body("Content-Type header must be " + MediaType.APPLICATION_JSON);
        }

        else if (!headers.containsKey("storage-type") ||
                (!headers.get("storage-type").equals("NATIVE") && !headers.get("storage-type").equals("HDFS")))
        {
            return ResponseEntity.badRequest().body("Storage-Type header must be either '" + StorageHandler.StorageType.NATIVE.name() +
                    "' or '" + StorageHandler.StorageType.HDFS.name() + "'");
        }

        else if (!headers.containsKey("signature-size") || !headers.containsKey("band-size"))
        {
            return ResponseEntity.badRequest().body("Missing LSH parameters ('Signature-Size', 'Band-Size')");
        }

        else if (!body.containsKey(dirKey))
        {
            return ResponseEntity.badRequest().body("Body must be a JSON string containing a single entry '" + dirKey + "'");
        }

        else if (!body.containsKey(tablePrefixKey))
        {
            return ResponseEntity.badRequest().body("Missing table entity prefix '" + tablePrefixKey + "' in JSON string");
        }

        else if (!body.containsKey(kgPrefixKey))
        {
            return ResponseEntity.badRequest().body("Missing table entity prefix '" + kgPrefixKey + "' in JSON string");
        }

        File dir = new File(body.get(dirKey));
        StorageHandler.StorageType storageType = StorageHandler.StorageType.valueOf(headers.get("storage-type"));
        Configuration.setStorageType(storageType);

        if (!dir.exists() || !dir.isDirectory())
        {
            return ResponseEntity.badRequest().body("'" + dir + "' is not a directory");
        }

        else if (this.indexLoadingInProgress)
        {
            return ResponseEntity.badRequest().body("Indexes are currently being build. Wait until finished");
        }

        try
        {
            long totalTime = System.nanoTime();
            KGService kgService = new KGService(Configuration.getEKGManagerHost(), Configuration.getEKGManagerPort());
            ELService elService = new ELService(Configuration.getEntityLinkerHost(), Configuration.getEntityLinkerPort());
            DBDriverBatch<List<Double>, String> embeddingStore = EmbeddingsFactory.fromConfig(false);
            this.indexLoadingInProgress = true;

            if (kgService.size() < 1)
            {
                Logger.log(Logger.Level.ERROR, "KG is empty. Make sure to load the KG according to README. Continuing...");
            }

            int signatureSize = Integer.parseInt(headers.get("signature-size")), bandSize = Integer.parseInt(headers.get("band-size"));
            Stream<Path> fileStream = Files.find(dir.toPath(), Integer.MAX_VALUE,
                    (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> filePaths = fileStream.collect(Collectors.toList());
            Configuration.setPermutationVectors(signatureSize);
            Configuration.setBandSize(bandSize);
            Logger.log(Logger.Level.INFO, "There are " + filePaths.size() + " files to be processed.");

            IndexWriter indexWriter = new IndexWriter(filePaths, new File(Configuration.getIndexDir()), DATA_DIR, storageType,
                    kgService, elService, embeddingStore, THREADS, body.get(tablePrefixKey), body.get(kgPrefixKey));
            indexWriter.performIO();
            embeddingStore.close();

            if (!kgService.insertLinks(DATA_DIR))
            {
                FileLogger.log(FileLogger.Service.EKG_Manager, "Failed inserting Turtle mapping files into the EKG service");
                Logger.log(Logger.Level.ERROR, "Failed inserting generated TTL mapping files into the EKG service");
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
            Logger.log(Logger.Level.INFO, "Found an approximate total of " + indexWriter.getApproximateEntityMentions() + " unique entity mentions across " + indexWriter.cellsWithLinks() + " cells \n");
            Logger.log(Logger.Level.INFO, "There are in total " + entityTypes.size() + " unique entity types across all discovered entities.");
            Logger.log(Logger.Level.INFO, "Indexing took " +
                    TimeUnit.SECONDS.convert(indexWriter.elapsedTime(), TimeUnit.NANOSECONDS) + "s");
            Configuration.setIndexesLoaded(true);
            loadIndexes();
            FileLogger.log(FileLogger.Service.SDL_Manager, "Loaded " + indexWriter.loadedTables() + " tables");
            analysis.record("insert", 1);
            this.indexLoadingInProgress = false;

            return ResponseEntity.ok("Loaded tables: " + indexWriter.loadedTables() + "\nIndex time: " +
                    TimeUnit.SECONDS.convert(indexWriter.elapsedTime(), TimeUnit.NANOSECONDS) + "s\nTotal elapsed time: " +
                    TimeUnit.SECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + "s");
        }

        catch (IOException e)
        {
            this.indexLoadingInProgress = false;
            Configuration.setIndexesLoaded(false);
            return ResponseEntity.badRequest().body("Error locating JSON table files: " + e.getMessage());
        }

        catch (RuntimeException e)
        {
            if (e.getMessage().contains("Postgres"))
            {
                Configuration.setEmbeddingsLoaded(false);
            }

            this.indexLoadingInProgress = false;
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
     * @return Basic stats
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
            if (this.embeddingsLoadingInProgress)
            {
                return ResponseEntity.badRequest().body("Embeddings are currently being loading. Wait until finished.");
            }

            EmbeddingsParser parser = new EmbeddingsParser(new FileInputStream(body.get("file")), body.get("delimiter").charAt(0));
            DBDriverBatch<List<Double>, String> db = EmbeddingsFactory.fromConfig(true);
            int batchSize = 100, batchSizeCount = batchSize;
            double loaded = 0;
            this.embeddingsLoadingInProgress = true;

            while (parser.hasNext())
            {
                int bytes = insertEmbeddings(db, parser, batchSize);
                loaded += (double) bytes / Math.pow(1024, 2);

                if (bytes == 0)
                    Logger.log(Logger.Level.ERROR, "INSERTION ERROR: " + ((ExplainableCause) db).getError());

                else
                    Logger.log(Logger.Level.INFO, "LOAD BATCH [" + batchSizeCount + "] - " + loaded + " mb");

                batchSizeCount += bytes > 0 ? batchSize : 0;
            }

            Configuration.setEmbeddingsLoaded(true);
            FileLogger.log(FileLogger.Service.SDL_Manager, "Loaded " + batchSizeCount +
                    " entity embeddings corresponding to " + loaded + " mb");
            db.close();
            analysis.record("embeddings", 1);
            this.embeddingsLoadingInProgress = false;

            return ResponseEntity.ok("Loaded " + batchSizeCount + " entity embeddings (" + loaded + " mb)");
        }

        catch (FileNotFoundException e)
        {
            this.embeddingsLoadingInProgress = false;
            Configuration.setEmbeddingsLoaded(false);
            return ResponseEntity.badRequest().body("Embeddings file error: " + e.getMessage());
        }

        catch (IllegalArgumentException e)
        {
            this.embeddingsLoadingInProgress = false;
            Configuration.setEmbeddingsLoaded(false);
            return ResponseEntity.badRequest().body("Could not initialize embeddings database: " + e.getMessage());
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

    /**
     * Remove all tables.
     * KG, embeddings, and indexes remain untouched.
     */
    @GetMapping("/clear")
    public synchronized ResponseEntity<String> clear(@RequestHeader Map<String, String> headers)
    {
        if (this.indexLoadingInProgress)
        {
            return ResponseEntity.badRequest().body("Indexes are currently being loaded");
        }

        StorageHandler storage = new StorageHandler(Configuration.getStorageType());

        if (!storage.clear())
        {
            return ResponseEntity.badRequest().body("Tables could not be removed: unknown reason");
        }

        return ResponseEntity.ok("Removed all tables successfully");
    }

    @GetMapping("/clear-embeddings")
    public synchronized ResponseEntity<String> clearEmbeddings(@RequestHeader Map<String, String> headers)
    {
        if (this.indexLoadingInProgress)
        {
            return ResponseEntity.badRequest().body("Indexes are currently being loaded");
        }

        try
        {
            DBDriverBatch<List<Double>, String> db = EmbeddingsFactory.fromConfig(false);

            if (!db.clear())
            {
                throw new IOException("Could not clear embeddings from DB");
            }

            embeddingsIndex.clear();
            IndexWriter.synchronizeIndexes(new File(Configuration.getIndexDir()), linker, entityTable, tableLink,
                    embeddingsIndex, typesLSH, embeddingLSH);
            Configuration.setIndexesLoaded(false);
            Configuration.setEmbeddingsLoaded(false);

            return ResponseEntity.ok("Embeddings have been cleared");
        }

        catch (IOException e)
        {
            return ResponseEntity.internalServerError().body("Failed updating indexes to disk: " + e.getMessage());
        }
    }
}