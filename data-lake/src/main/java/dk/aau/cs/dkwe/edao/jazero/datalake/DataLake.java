package dk.aau.cs.dkwe.edao.jazero.datalake;

import dk.aau.cs.dkwe.edao.jazero.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.EmbeddingsFactory;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.ExplainableCause;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.IndexReader;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.IndexWriter;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive.Event;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive.PriorityScheduler;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive.ProgressiveIndexWriter;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.EmbeddingsParser;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Prefilter;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh.SetLSHIndex;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh.VectorLSHIndex;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.StorageHandler;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.*;
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
    private static SetLSHIndex typesLSH;
    private static VectorLSHIndex embeddingLSH;
    private static EndpointAnalysis analysis;
    private static final int THREADS = 4;
    private static final File DATA_DIR = new File("/index/mappings/");
    private boolean indexLoadingInProgress = false, progressiveLoadingInProgress = false, embeddingsLoadingInProgress = false;
    private static IndexWriter indexer;

    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(Configuration.getSDLManagerPort());
    }

    public static void main(String[] args)
    {
        FileLogger.log(FileLogger.Service.SDL_Manager, "Starting...");
        loadIndexes(true);

        FileLogger.log(FileLogger.Service.SDL_Manager, "Started");
        FileLogger.log(FileLogger.Service.SDL_Manager, "SDL Manager available at 0.0.0.0:" + Configuration.getSDLManagerPort());
        SpringApplication.run(DataLake.class, args);
    }

    private static void loadIndexes(boolean fromDisk)
    {
        analysis = new EndpointAnalysis();

        if (Configuration.areIndexesLoaded())
        {
            if (fromDisk)
            {
                try
                {
                    IndexReader indexReader = new IndexReader(new File(Configuration.getIndexDir()), true, true);
                    indexReader.performIO();

                    linker = indexReader.getLinker();
                    entityTable = indexReader.getEntityTable();
                    tableLink = indexReader.getEntityTableLink();
                    typesLSH = indexReader.getTypesLSH();
                    embeddingLSH = indexReader.getVectorsLSH();
                }

                catch (IOException | RuntimeException e)
                {
                    FileLogger.log(FileLogger.Service.SDL_Manager, "Failed loading indexes: " + e.getMessage());
                    Configuration.setIndexesLoaded(false);
                }
            }

            else if (indexer != null)
            {
                linker = indexer.getEntityLinker();
                entityTable = indexer.getEntityTable();
                tableLink = indexer.getEntityTableLinker();
                typesLSH = indexer.getTypesLSH();
                embeddingLSH = indexer.getEmbeddingsLSH();
            }

            typesLSH.useEntityLinker(linker);
            typesLSH.useEntityTable(entityTable);
            embeddingLSH.useEntityLinker(linker);
            embeddingLSH.useEntityTable(entityTable);
        }
    }

    private static Authenticator.Auth authenticateUser(Map<String, String> headers)
    {
        Authenticator auth = Configuration.initAuthenticator();

        if (!headers.containsKey("username") || !headers.containsKey("password"))
        {
            return Authenticator.Auth.NOT_AUTH;
        }

        return auth.authenticate(headers.get("username"), headers.get("password"));
    }

    /**
     * Used to verify service is running
     */
    @GetMapping(value = "/ping")
    public ResponseEntity<String> ping(@RequestHeader Map<String, String> headers)
    {
        if (authenticateUser(headers) == Authenticator.Auth.NOT_AUTH)
        {
            return ResponseEntity.badRequest().body("User could not be authenticated");
        }

        analysis.record("ping", 1);
        Logger.log(Logger.Level.INFO, "PING");
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    /**
     * POST request to query data lake.
     * @param headers Requires:
     *                "Content-Type": "application/json",
     *                "username": "<USERNAME>",
     *                "password": "<PASSWORD>"
     * @param body Query as JSON string on the form:
     *             {
     *                  "top-k": "<INTEGER VALUE>",
     *                  "entity-similarity": "TYPES|PREDICATES|EMBEDDINGS",
     *                  "single-column-per-query-entity": "<BOOLEAN VALUE>",
     *                  "use-max-similarity-per-column": "<BOOLEAN VALUE>",
     *                  ["weighted-jaccard": "<BOOLEAN VALUE>,]
     *                  ["cosine-function": "NORM_COS|ABS_COS|ANG_COS",]
     *                  ["lsh": "TYPES|EMBEDDINGS",]
     *                  ["query-time": <TIME IN SECONDS>]
     *                  "query": "<QUERY STRING>"
     *             }
     * <p>
     *             The <QUERY STRING> is a list of tuples, each tuple separated by a hashtag (#),
     *             and each tuple element is separated by a diamond (<>).
     * @return JSON array of found tables. Each element is a pair of table ID and score.
     */
    // TODO: Check if progressive indexing is in progress. If so, retrieve the partially constructed indexes from the ProgressiveIndexWriter class.
    @PostMapping(value = "/search")
    public ResponseEntity<String> search(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body) throws InterruptedException
    {
        if (authenticateUser(headers) == Authenticator.Auth.NOT_AUTH)
        {
            return ResponseEntity.badRequest().body("User could not be authenticated");
        }

        else if (!Configuration.areIndexesLoaded())
        {
            return ResponseEntity.badRequest().body("Indexes have not been loaded. Use the '/insert' endpoint.");
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
        int queryTime = Integer.parseInt(body.getOrDefault("query-time", "0"));
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
                prefilter = new Prefilter(linker, entityTable, tableLink, typesLSH);
            }

            else if (lshType.equals("EMBEDDINGS"))
            {
                prefilter = new Prefilter(linker, entityTable, tableLink, embeddingLSH);
            }
        }

        if (this.progressiveLoadingInProgress)
        {
            TimeUnit.SECONDS.sleep(queryTime);
            ((ProgressiveIndexWriter) indexer).pauseIndexing();
            loadIndexes(false);
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
            IndexWriter.indexKGEntity(entity, linker, entityTable, kgService, embeddingsDB);
        }

        if (prefilter != null)
        {
            search = new TableSearch(storageHandler, linker, entityTable, tableLink, topK, THREADS, entitySimilarity,
                    singleColumnPerEntity, weightedJaccard, useMaxSimilarityPerColumn, false, similarityMeasure, prefilter);
        }

        else
        {
            search = new TableSearch(storageHandler, linker, entityTable, tableLink, topK, THREADS, entitySimilarity,
                    singleColumnPerEntity, weightedJaccard, useMaxSimilarityPerColumn, false, similarityMeasure);
        }

        Result result = search.search(query);

        if (this.progressiveLoadingInProgress)
        {
            Iterator<Pair<File, Double>> resultIter = result.getResults();
            ((ProgressiveIndexWriter) indexer).continueIndexing();

            while (resultIter.hasNext())
            {
                Pair<File, Double> table = resultIter.next();
                String id = table.first().getName();
                double score = table.second(), alpha = 1;   // TODO: Compute the correct alpha
                ((ProgressiveIndexWriter) indexer).updatePriority(id, (i) -> i.setPriority(i.getPriority() * (1 + score * alpha)));
            }
        }

        if (result == null)
        {
            FileLogger.log(FileLogger.Service.SDL_Manager, "Search result set is null");
            return ResponseEntity.internalServerError().body("Result set is null");
        }

        return ResponseEntity
                .ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(result.toString());
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

    /**
     * POST request to load data lake
     * Make sure to only use this once as it will delete any previously loaded data
     * 'Signature-Size' is number of permutation/projection vectors to be used in LSH index
     * 'Band-Size' is size of bands for the LSH signature
     * @param headers Requires:
     *                "Content-Type": "application/json",
     *                "Storage-Type": "native|HDFS",
     *                "Signature-Size": <INTEGER>,
     *                "Band-Size": <INTEGER>,
     *                "username": "<USERNAME>",
     *                "password": "<PASSWORD>"
     *
     * @param body JSON string with path to directory of JSON table files. Format:
     *             {
     *                  "directory": "<DIRECTORY>",
     *                  "table-prefix": "<TABLE PREFIX>",
     *                  "kg-prefix": "<KG PREFIX>",
     *                  ["progressive": "<BOOLEAN VALUE>"]
     *             }
     * <p>
     *             "table-prefix" is the common prefix for table entities, just like kg-prefix. If the prefix differs
     *             from entity to entity, use the empty string.
     *             Optionally, an entry 'disallowed_types' for a JSON array of entity types can be added to indicated
     *             entity types that should be removed
     * @return Simple index build stats
     */
    @PostMapping(value = "/insert")
    public synchronized ResponseEntity<String> insert(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
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

        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (!Configuration.areEmbeddingsLoaded())
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

        boolean isProgressive = Boolean.parseBoolean(body.getOrDefault("progressive", "false"));
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

            if (isProgressive)
            {
                this.progressiveLoadingInProgress = true;
                Runnable cleanup = () -> {
                    this.indexLoadingInProgress = false;
                    this.progressiveLoadingInProgress = false;
                    embeddingStore.close();
                    Configuration.setIndexesLoaded(true);
                    loadIndexes(false);
                    FileLogger.log(FileLogger.Service.SDL_Manager, "Progressively loaded " + indexer.loadedTables() + " tables in " +
                            TimeUnit.SECONDS.convert(indexer.elapsedTime(), TimeUnit.NANOSECONDS) + "s");
                    analysis.record("insert-progressive", 1);
                };
                indexer = new ProgressiveIndexWriter(filePaths, new File(Configuration.getIndexDir()), DATA_DIR,
                        storageType, kgService, elService, embeddingStore, THREADS, body.get(tablePrefixKey), body.get(kgPrefixKey),
                        new PriorityScheduler(), cleanup);
                indexer.performIO();
                Logger.log(Logger.Level.INFO, "Started progressive loading of " + filePaths.size() + " tables");

                return ResponseEntity.ok("Started progressive loading of " + filePaths.size() + " tables");
            }

            indexer = new IndexWriter(filePaths, new File(Configuration.getIndexDir()), DATA_DIR, storageType,
                    kgService, elService, embeddingStore, THREADS, body.get(tablePrefixKey), body.get(kgPrefixKey));
            indexer.performIO();
            embeddingStore.close();

            if (!kgService.insertLinks(DATA_DIR))
            {
                FileLogger.log(FileLogger.Service.EKG_Manager, "Failed inserting Turtle mapping files into the EKG service");
                Logger.log(Logger.Level.ERROR, "Failed inserting generated TTL mapping files into the EKG service");
            }

            Set<Type> entityTypes = new HashSet<>();
            Iterator<Id> idIter = indexer.getEntityLinker().uriIds();

            while (idIter.hasNext())
            {
                Entity entity = indexer.getEntityTable().find(idIter.next());

                if (entity != null)
                    entityTypes.addAll(entity.getTypes());
            }

            totalTime = System.nanoTime() - totalTime;
            Logger.log(Logger.Level.INFO, "Found an approximate total of " + indexer.getApproximateEntityMentions() +
                    " unique entity mentions across " + indexer.cellsWithLinks() + " cells \n");
            Logger.log(Logger.Level.INFO, "There are in total " + entityTypes.size() + " unique entity types across all discovered entities.");
            Logger.log(Logger.Level.INFO, "Indexing took " +
                    TimeUnit.SECONDS.convert(indexer.elapsedTime(), TimeUnit.NANOSECONDS) + "s");
            Configuration.setIndexesLoaded(true);
            loadIndexes(true);
            FileLogger.log(FileLogger.Service.SDL_Manager, "Loaded " + indexer.loadedTables() + " tables in " +
                    TimeUnit.SECONDS.convert(indexer.elapsedTime(), TimeUnit.NANOSECONDS) + "s");
            analysis.record("insert", 1);
            this.indexLoadingInProgress = false;

            return ResponseEntity.ok("Loaded tables: " + indexer.loadedTables() + "\nIndex time: " +
                    TimeUnit.SECONDS.convert(indexer.elapsedTime(), TimeUnit.NANOSECONDS) + "s\nTotal elapsed time: " +
                    TimeUnit.SECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + "s");
        }

        catch (IOException e)
        {
            this.indexLoadingInProgress = false;
            Configuration.setIndexesLoaded(false);
            FileLogger.log(FileLogger.Service.SDL_Manager, "IOException when loading tables");
            return ResponseEntity.badRequest().body("Error locating table files: " + e.getMessage());
        }

        catch (RuntimeException e)
        {
            if (e.getMessage().contains("Postgres"))
            {
                Configuration.setEmbeddingsLoaded(false);
            }

            this.indexLoadingInProgress = false;
            Configuration.setIndexesLoaded(false);
            FileLogger.log(FileLogger.Service.SDL_Manager, "RuntimeException when loading tables");
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        }
    }

    /**
     * POST request to load embeddings
     * In the embeddings file, each entity and embedding must be separated by new line
     * In each line, start with the entity URI and follow by its embedding values
     * Use the same delimiter to separate embedding values and entity URI from its embedding
     * @param headers Requires:
     *                "Content-Type": "application/json",
     *                "username": "<USERNAME>",
     *                "password": "<PASSWORD>"
     * @param body Requires JSON with two entries:
     *             {
     *                  "file": "<PATH/TO/EMBEDDINGS>"
     *                  "delimiter: " "<Delimiter separating each floating-point value and entity string>"
     *             }
     * @return Basic stats
     */
    @PostMapping(value = "/embeddings")
    public synchronized ResponseEntity<String> loadEmbeddings(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
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
            FileLogger.log(FileLogger.Service.SDL_Manager, "FileNotFoundException when loading embeddings");
            return ResponseEntity.badRequest().body("Embeddings file error: " + e.getMessage());
        }

        catch (IllegalArgumentException e)
        {
            this.embeddingsLoadingInProgress = false;
            Configuration.setEmbeddingsLoaded(false);
            FileLogger.log(FileLogger.Service.SDL_Manager, "IllegalArgumentException when loading embeddings");
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
     * Remove all tables and clears all indexes.
     * KG remains untouched.
     */
    @GetMapping("/clear")
    public synchronized ResponseEntity<String> clear(@RequestHeader Map<String, String> headers)
    {
        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (this.indexLoadingInProgress)
        {
            return ResponseEntity.badRequest().body("Indexes are currently being loaded");
        }

        StorageHandler storage = new StorageHandler(Configuration.getStorageType());
        Logger.log(Logger.Level.INFO, "Removing tables");

        if (!storage.clear())
        {
            return ResponseEntity.badRequest().body("Tables could not be removed: unknown reason");
        }

        try
        {
            Logger.log(Logger.Level.INFO, "Clearing indexes");

            if (Configuration.areIndexesLoaded())
            {
                linker.clear();
                entityTable.clear();
                tableLink.clear();
                typesLSH.clear();
                embeddingLSH.clear();
                IndexWriter.synchronizeIndexes(new File(Configuration.getIndexDir()), linker, entityTable, tableLink,
                        typesLSH, embeddingLSH);
            }

            Configuration.setIndexesLoaded(false);
            FileLogger.log(FileLogger.Service.SDL_Manager, "Cleared all indexes and " + storage.count() + " tables");
        }

        catch (IOException e)
        {
            return ResponseEntity.internalServerError().body("Failed updating cleared indexes to disk: " + e.getMessage());
        }

        return ResponseEntity.ok("Removed all tables and cleared all indexes successfully");
    }

    /**
     * Removes embeddings from DB but not index
     */
    @GetMapping("/clear-embeddings")
    public synchronized ResponseEntity<String> clearEmbeddings(@RequestHeader Map<String, String> headers)
    {
        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (this.indexLoadingInProgress)
        {
            return ResponseEntity.badRequest().body("Indexes are currently being loaded");
        }

        try
        {
            DBDriverBatch<List<Double>, String> db = EmbeddingsFactory.fromConfig(false);
            Logger.log(Logger.Level.INFO, "Clearing embeddings DB");

            if (!db.clear())
            {
                throw new IOException("Could not clear embeddings from DB");
            }

            Configuration.setEmbeddingsLoaded(false);
            Logger.log(Logger.Level.INFO, "Done");
            FileLogger.log(FileLogger.Service.SDL_Manager, "Cleared all embeddings");

            return ResponseEntity.ok("Embeddings from DB have been cleared");
        }

        catch (IOException e)
        {
            return ResponseEntity.internalServerError().body("Failed updating indexes to disk: " + e.getMessage());
        }
    }

    /**
     * Removes a single table from the data lake
     * Body must contain entry 'table' which is the identifier of the table with file extension
     */
    @PostMapping(value = "/remove-table")
    public synchronized ResponseEntity<String> removeTable(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        String key = "table";

        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (!body.containsKey(key))
        {
            return ResponseEntity.badRequest().body("Missing '" + key + "' key in JSON body");
        }

        String tableId = body.get(key);
        StorageHandler storageHandler = new StorageHandler(Configuration.getStorageType());

        if (!storageHandler.delete(new File(tableId)))
        {
            return ResponseEntity.badRequest().body("Table '" + tableId + "' was not deleted. Maybe it doesn't exist?");
        }

        // TODO: Remove also from LSH indexes and re-serialize

        Logger.log(Logger.Level.INFO, "Removed table '" + tableId + "'");
        FileLogger.log(FileLogger.Service.SDL_Manager, "Table '" + tableId + "' has been removed");

        return ResponseEntity.ok("Table '" + tableId + "' was deleted successfully");
    }

    /**
     * Adds a user to the system
     * All users are assigned write privileges
     */
    @PostMapping("/add-user")
    public synchronized ResponseEntity<String> addUser(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (!body.containsKey("new-username") || !body.containsKey("new-password"))
        {
            return ResponseEntity.badRequest().body("Missing new user to be added");
        }

        Authenticator auth = Configuration.initAuthenticator();
        String newUsername = body.get("new-username"), newPassword = body.get("new-password");

        if (auth.authenticate(newUsername, newPassword) != Authenticator.Auth.NOT_AUTH)
        {
            return ResponseEntity.badRequest().body("Username is already taken");
        }

        auth.allow(new User(newUsername, newPassword, false));
        Logger.log(Logger.Level.INFO, "Added user '" + newUsername + "'");
        FileLogger.log(FileLogger.Service.SDL_Manager, "New user '" + newUsername + "' was added");

        return ResponseEntity.ok("User '" + newUsername + "' has been added");
    }

    /**
     * Removes a user from the system
     */
    @PostMapping("/remove-user")
    public synchronized ResponseEntity<String> removeUser(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (authenticateUser(headers) != Authenticator.Auth.WRITE)
        {
            return ResponseEntity.badRequest().body("User does not have write privileges");
        }

        else if (!body.containsKey("old-username"))
        {
            return ResponseEntity.badRequest().body("Missing username of user to be removed");
        }

        String oldUsername = body.get("old-username");
        Authenticator auth = Configuration.initAuthenticator();
        auth.disallow(oldUsername);
        Logger.log(Logger.Level.INFO, "Removed user '" + oldUsername + "'");
        FileLogger.log(FileLogger.Service.SDL_Manager, "User '" + oldUsername + "' was removed");

        return ResponseEntity.ok("User '" + oldUsername + "' has been removed");
    }

    /**
     * Adds the admin user.
     * This endpoint can only be reached once sor security reasons.
     * The body requires two fields, 'username' and 'password', both of which are strings to set the admin login credentials.
     */
    @PostMapping("/set-admin")
    public synchronized ResponseEntity<String> setAdmin(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (Configuration.isAdminSet())
        {
            return ResponseEntity.badRequest().body("Admin is already set");
        }

        else if (!body.containsKey("username") && !body.containsKey("password"))
        {
            return ResponseEntity.badRequest().body("Missing fields 'username' and/or 'password' to set admin credentials");
        }

        User admin = new User(body.get("username"), body.get("password"), false);
        Authenticator auth = Configuration.initAuthenticator();
        auth.allow(admin);
        Configuration.setAdmin();
        Logger.log(Logger.Level.INFO, "Admin has been set");
        FileLogger.log(FileLogger.Service.SDL_Manager, "Admin has been set");

        return ResponseEntity.ok("User '" + admin.username() + "' has been set as admin");
    }
}