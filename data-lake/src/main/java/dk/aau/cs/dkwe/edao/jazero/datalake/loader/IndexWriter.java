package dk.aau.cs.dkwe.edao.jazero.datalake.loader;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.ParsingException;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.*;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.hnsw.HNSW;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Embedding;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.PairNonComparable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.FileLogger;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.jazero.datalake.tables.JsonTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.utilities.Utils;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.StorageHandler;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IndexWriter implements IndexIO
{
    protected List<Path> files;
    protected final File indexDir, dataDir;
    protected final StorageHandler storage;
    protected final int threads;
    protected AtomicLong loadedTables = new AtomicLong(0);
    protected final AtomicInteger  cellsWithLinks = new AtomicInteger(0), tableStatsCollected = new AtomicInteger(0);
    protected final Object lock = new Object(), incrementLock = new Object();
    protected long elapsed = -1;
    private final KGService kg;
    private final ELService el;
    protected final SynchronizedLinker<String, String> linker;
    protected final SynchronizedIndex<Id, Entity> entityTable;
    protected final SynchronizedIndex<Id, List<String>> entityTableLink;
    protected final SynchronizedIndex<String, Set<String>> hnsw;
    private final DBDriverBatch<List<Double>, String> embeddingsDB;
    protected final BloomFilter<String> filter = BloomFilter.create(
            Funnels.stringFunnel(Charset.defaultCharset()),
            5_000_000,
            0.01);
    protected final Map<String, Stats> tableStats = new TreeMap<>();

    private static final List<String> DISALLOWED_ENTITY_TYPES =
            Arrays.asList("http://www.w3.org/2002/07/owl#Thing", "http://www.wikidata.org/entity/Q5");
    private static final String STATS_DIR = "statistics/";
    protected static final int HNSW_K = 10000;

    public IndexWriter(List<Path> files, File indexPath, File dataOutputPath, StorageHandler.StorageType storageType, KGService kgService,
                       ELService elService, DBDriverBatch<List<Double>, String> embeddingStore, int threads, String wikiPrefix,
                       String uriPrefix)
    {
        if (files.isEmpty())
            throw new IllegalArgumentException("Missing files to load");

        this.files = files;
        this.indexDir = indexPath;
        this.dataDir = dataOutputPath;
        this.storage = new StorageHandler(storageType);
        this.embeddingsDB = embeddingStore;
        this.kg = kgService;
        this.el = elService;
        this.threads = threads;
        this.linker = SynchronizedLinker.wrap(new EntityLinking(wikiPrefix, uriPrefix));
        this.entityTable = SynchronizedIndex.wrap(new EntityTable());
        this.entityTableLink = SynchronizedIndex.wrap(new EntityTableLink());
        this.hnsw = SynchronizedIndex.wrap(new HNSW(Entity::getEmbedding, Configuration.getEmbeddingsDimension(),
                kgService.size(), HNSW_K, getEntityLinker(), getEntityTable(), getEntityTableLinker(), indexPath + "/" + Configuration.getHNSWFile()));
        ((EntityTableLink) this.entityTableLink.index()).setDirectory(files.get(0).toFile().getParent() + "/");
    }

    /**
     * Loading of tables to disk
     */
    @Override
    public void performIO() throws IOException
    {
        if (this.loadedTables.get() > 0)
            throw new RuntimeException("Loading has already complete");

        int size = this.files.size();
        ExecutorService threadPool = Executors.newFixedThreadPool(this.threads);
        List<Future<Boolean>> tasks = new ArrayList<>(size);
        long startTime = System.nanoTime(), prev = 0;

        for (int i = 0; i < size; i++)
        {
            final int index = i;
            Future<Boolean> task = threadPool.submit(() -> load(this.files.get(index)));
            tasks.add(task);
        }

        while (this.loadedTables.get() < size)
        {
            synchronized (this.incrementLock)
            {
                this.loadedTables.set(tasks.stream().filter(Future::isDone).count());
            }

            if (this.loadedTables.get() - prev >= 100)
            {
                Logger.log(Logger.Level.INFO, "Processed " + this.loadedTables.get() + "/" + size + " files...");
                prev = this.loadedTables.get();
            }
        }

        tasks.forEach(t ->
        {
            try
            {
                t.get();
            }

            catch (InterruptedException | ExecutionException ignored) {}
        });
        Logger.log(Logger.Level.INFO, "Collecting IDF weights...");
        loadIDFs();
        Logger.log(Logger.Level.INFO, "Writing indexes and stats on disk...");
        writeStats();
        this.tableStats.clear();    // Save memory before writing index objects to disk
        synchronizeIndexes(this.indexDir, this.linker.linker(), this.entityTable.index(), this.entityTableLink.index(), this.hnsw.index());
        genNeo4jTableMappings();

        this.elapsed = System.nanoTime() - startTime;
        Logger.log(Logger.Level.INFO, "Done");
        Logger.log(Logger.Level.INFO, "A total of " + this.loadedTables.get() + " tables were loaded");
        Logger.log(Logger.Level.INFO, "Elapsed time: " + this.elapsed / (1e9) + " seconds");
    }

    /**
     * Quick linking of entity
     * @param entity Entity to be linked
     * @param linker Entity linking index
     * @param entityTable EntityTable index
     * @param el Entity linker service
     * @param kg KG service
     */
    public static synchronized String indexEntity(String entity, EntityLinking linker, EntityTable entityTable, HNSW hnsw,
                                                  ELService el, KGService kg, DBDriverBatch<List<Double>, String> embeddingsDB)
    {
        String uri = linker.mapTo(entity);

        if (uri == null)
        {
            uri = el.link(entity);

            if (uri != null)
            {
                linker.addMapping(entity, uri);
                indexKGEntity(uri, linker, entityTable, hnsw, kg, embeddingsDB);
            }
        }

        return uri;
    }

    protected String indexEntity(String entity)
    {
        return indexEntity(entity, ((EntityLinking) this.linker.linker()), ((EntityTable) this.entityTable.index()),
                (HNSW) this.hnsw.index(), this.el, this.kg, this.embeddingsDB);
    }

    /**
     * Indexing of query entity of KG entities
     * @param uri KG entity
     * @param linker Entity linking indes
     * @param entityTable Entity index
     * @param kg KG service
     * @param embeddingsDB Embeddings database
     */
    public static void indexKGEntity(String uri, EntityLinking linker, EntityTable entityTable, HNSW hnsw, KGService kg,
                                     DBDriverBatch<List<Double>, String> embeddingsDB)
    {
        Id entityId = linker.uriLookup(uri);

        if (entityId != null && entityTable.contains(entityId))
        {
            return;
        }

        List<String> types = kg.searchTypes(uri), predicates = kg.searchPredicates(uri);
        types.removeAll(DISALLOWED_ENTITY_TYPES);

        List<Double> embeddings = embeddingsDB.select(uri.replace("'", "''"));
        Embedding e = embeddings == null ? null : new Embedding(embeddings);
        Entity entity = new Entity(uri, types.stream().map(Type::new).collect(Collectors.toList()), predicates, e);
        linker.addMapping("###BLANK###", uri);  // This depends on the addMapping() implementation and ensures that the URI is assigned an ID

        entityId = linker.uriLookup(uri);
        entityTable.insert(entityId, entity);
        hnsw.insert(uri, Collections.emptySet());   // The index does not require a list of tables, it is ignored
    }

    protected void indexKGEntity(String uri)
    {
        indexKGEntity(uri, (EntityLinking) this.linker.linker(), (EntityTable) this.entityTable.index(), (HNSW) this.hnsw.index(), this.kg, this.embeddingsDB);
    }

    /**
     * Updates indexes with new entities
     * @param entities Set of entities and their table location
     * @return Mapping from location to KG URI
     */
    protected Map<Pair<Integer, Integer>, List<String>> update(Set<PairNonComparable<String, Pair<Integer, Integer>>> entities, String tableName)
    {
        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();

        for (PairNonComparable<String, Pair<Integer, Integer>> entity : entities)
        {
            String mention = entity.first();
            Pair<Integer, Integer> location = entity.second();
            String uri = indexEntity(mention);
            List<String> matchesUris = new ArrayList<>();
            this.cellsWithLinks.incrementAndGet();

            if (uri != null)
            {
                Id entityId = ((EntityLinking) this.linker.linker()).uriLookup(uri);
                matchesUris.add(uri);

                synchronized (this.lock)
                {
                    ((EntityTableLink) this.entityTableLink.index()).
                            addLocation(entityId, tableName, List.of(location));
                }
            }

            if (!matchesUris.isEmpty())
            {
                synchronized (this.lock)
                {
                    matchesUris.forEach(this.filter::put);
                }

                entityMatches.put(location, matchesUris);
            }
        }

        return entityMatches;
    }

    private boolean load(Path tablePath)
    {
        JsonTable table = parseTable(tablePath);

        if (table == null ||  table._id == null || table.rows == null)
            return false;

        String tableName = tablePath.getFileName().toString();
        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();  // Maps a cell specified by RowNumber, ColumnNumber to the list of entities it matches to
        Table<String> inputEntities = new DynamicTable<>();
        int row = 0;

        for (List<JsonTable.TableCell> tableRow : table.rows)
        {
            int column = 0;
            Set<PairNonComparable<String, Pair<Integer, Integer>>> rowEntities = new HashSet<>();

            for (JsonTable.TableCell cell : tableRow)
            {
                String cellText = cell.text;
                rowEntities.add(new PairNonComparable<>(cellText, new Pair<>(row, column)));
                column++;
            }

            Map<Pair<Integer, Integer>, List<String>> linking = update(rowEntities, tableName);
            List<Map.Entry<Pair<Integer, Integer>, List<String>>> linkedRow = new ArrayList<>(linking.entrySet());
            linkedRow.sort(Comparator.comparingInt(e -> e.getKey().second()));
            inputEntities.addRow(new Table.Row<>(linkedRow.stream().map(e -> e.getValue().get(0)).collect(Collectors.toList())));
            entityMatches.putAll(linking);
            row++;
        }

        synchronized (this.lock)
        {
            saveStats(table, FilenameUtils.removeExtension(tableName), inputEntities.iterator(), entityMatches);
            this.storage.insert(tablePath.toFile());
        }

        return true;
    }

    private static JsonTable parseTable(Path tablePath)
    {
        try
        {
            return TableParser.parse(tablePath);
        }

        catch (ParsingException e)
        {
            FileLogger.log(FileLogger.Service.SDL_Manager, e.getMessage());
            Logger.log(Logger.Level.ERROR, e.getMessage());
            return null;
        }
    }

    private void saveStats(JsonTable jTable, String tableFileName, Iterator<String> entities, Map<Pair<Integer, Integer>, List<String>> entityMatches)
    {
        Stats stats = collectStats(jTable, tableFileName, entities, entityMatches);
        this.tableStats.put(tableFileName, stats);
    }

    private Stats collectStats(JsonTable jTable, String tableFileName, Iterator<String> entities, Map<Pair<Integer, Integer>, List<String>> entityMatches)
    {
        List<Integer> numEntitiesPerRow = new ArrayList<>(Collections.nCopies(jTable.numDataRows, 0));
        List<Integer> numEntitiesPerCol = new ArrayList<>(Collections.nCopies(jTable.numCols, 0));
        List<Integer> numCellToEntityMatchesPerCol = new ArrayList<>(Collections.nCopies(jTable.numCols, 0));
        List<Boolean> tableColumnsIsNumeric = new ArrayList<>(Collections.nCopies(jTable.numCols, false));
        long numCellToEntityMatches = 0L; // Specifies the total number (bag semantics) of entities all cells map to
        int entityCount = 0;

        while (entities.hasNext())
        {
            entityCount++;
            Id entityId = ((EntityLinking) this.linker.linker()).uriLookup(entities.next());

            if (entityId == null)
                continue;

            List<Pair<Integer, Integer>> locations =
                    ((EntityTableLink) this.entityTableLink.index()).getLocations(entityId, tableFileName);

            if (locations != null)
            {
                for (Pair<Integer, Integer> location : locations)
                {
                    numEntitiesPerRow.set(location.first(), numEntitiesPerRow.get(location.first()) + 1);
                    numEntitiesPerCol.set(location.second(), numEntitiesPerCol.get(location.second()) + 1);
                    numCellToEntityMatches++;
                }
            }
        }

        for (Pair<Integer, Integer> position : entityMatches.keySet())
        {
            Integer colId = position.second();
            numCellToEntityMatchesPerCol.set(colId, numCellToEntityMatchesPerCol.get(colId) + 1);
        }

        if (jTable.numNumericCols == jTable.numCols)
            tableColumnsIsNumeric = new ArrayList<>(Collections.nCopies(jTable.numCols, true));

        else if (!jTable.rows.isEmpty())
        {
            int colId = 0;

            for (JsonTable.TableCell cell : jTable.rows.get(0))
            {
                if (cell.isNumeric)
                    tableColumnsIsNumeric.set(colId, true);

                colId++;
            }
        }

        this.tableStatsCollected.incrementAndGet();
        return Stats.build()
                .rows(jTable.numDataRows)
                .columns(jTable.numCols)
                .cells(jTable.numDataRows * jTable.numCols)
                .entities(entityCount)
                .mappedCells(entityMatches.size())
                .entitiesPerRow(numEntitiesPerRow)
                .entitiesPerColumn(numEntitiesPerCol)
                .cellToEntityMatches(numCellToEntityMatches)
                .cellToEntityMatchesPerCol(numCellToEntityMatchesPerCol)
                .numericTableColumns(tableColumnsIsNumeric)
                .finish();
    }

    private void writeStats()
    {
        File statDir = new File(this.indexDir + "/" + STATS_DIR);

        if (!statDir.exists())
            statDir.mkdir();

        try
        {
            FileWriter writer = new FileWriter(statDir + "/" + Configuration.getTableStatsFile());
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(this.tableStats, writer);
            writer.flush();
            writer.close();
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    protected void loadIDFs()
    {
        loadEntityIDFs();
        loadTypeIDFs();
    }

    private void loadEntityIDFs()
    {
        Iterator<Id> idIter = ((EntityLinking) this.linker.linker()).uriIds();

        while (idIter.hasNext())
        {
            Id entityId = idIter.next();
            double idf = Math.log10((double) this.loadedTables.get() / this.entityTableLink.find(entityId).size()) + 1;

            if (!this.entityTableLink.find(entityId).isEmpty() && this.entityTable.contains(entityId))
            {
                this.entityTable.find(entityId).setIDF(idf);
            }
        }
    }

    private void loadTypeIDFs()
    {
        Map<Type, Integer> entityTypeFrequency = new HashMap<>();
        Iterator<Id> idIterator = ((EntityLinking) this.linker.linker()).uriIds();

        while (idIterator.hasNext())
        {
            Id id = idIterator.next();

            if (this.entityTable.contains(id))
            {
                List<Type> entityTypes = this.entityTable.find(id).getTypes();

                for (Type t : entityTypes)
                {
                    if (entityTypeFrequency.containsKey(t))
                        entityTypeFrequency.put(t, entityTypeFrequency.get(t) + 1);

                    else
                        entityTypeFrequency.put(t, 1);
                }
            }
        }

        long totalEntityCount = this.entityTable.size();
        idIterator = ((EntityLinking) this.linker.linker()).uriIds();

        while (idIterator.hasNext())
        {
            Id id = idIterator.next();

            if (this.entityTable.contains(id))
            {
                this.entityTable.find(id).getTypes().forEach(t -> {
                    if (entityTypeFrequency.containsKey(t))
                    {
                        double idf = Utils.log2((double) totalEntityCount / entityTypeFrequency.get(t));
                        t.setIdf(idf);
                    }
                });
            }
        }
    }

    public synchronized static void synchronizeIndexes(File indexDir, Linker<String, String> linker,
                                                       Index<Id, Entity> entityTable, Index<Id, List<String>> entityTableLink,
                                                       Index<String, Set<String>> hnsw) throws IOException
    {
        // Entity linker
        ObjectOutputStream outputStream =
                new ObjectOutputStream(new FileOutputStream(indexDir + "/" + Configuration.getEntityLinkerFile()));
        outputStream.writeObject(linker);
        outputStream.flush();
        outputStream.close();

        // Entity table
        outputStream = new ObjectOutputStream(new FileOutputStream(indexDir + "/" + Configuration.getEntityTableFile()));
        outputStream.writeObject(entityTable);
        outputStream.flush();
        outputStream.close();

        // Entity to tables inverted index
        outputStream = new ObjectOutputStream(new FileOutputStream(indexDir + "/" + Configuration.getEntityToTablesFile()));
        outputStream.writeObject(entityTableLink);
        outputStream.flush();
        outputStream.close();

        // HNSW
        HNSW tmpHNSW = (HNSW) hnsw;
        outputStream = new ObjectOutputStream(new FileOutputStream(indexDir + "/" + Configuration.getHNSWParamsFile()));
        outputStream.writeInt(tmpHNSW.getEmbeddingsDimension());
        outputStream.writeLong(tmpHNSW.getCapacity());
        outputStream.writeInt(tmpHNSW.getNeighborhoodSize());
        outputStream.writeUTF(tmpHNSW.getIndexPath());
        outputStream.flush();
        outputStream.close();
        tmpHNSW.save();
    }

    protected void genNeo4jTableMappings() throws IOException
    {
        FileOutputStream outputStream = new FileOutputStream(this.dataDir + "/" + Configuration.getTableToEntitiesFile());
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        Iterator<Id> entityIter = ((EntityLinking) this.linker.linker()).uriIds();

        while (entityIter.hasNext())
        {
            Id entityId = entityIter.next();
            List<String> tables = this.entityTableLink.find(entityId);

            for (String table : tables)
            {
                writer.write("<http://thetis.edao.eu/wikitables/" + table +
                        "> <https://schema.org/mentions> <" + this.entityTable.find(entityId) + "> .\n");
            }
        }

        writer.flush();
        writer.close();
        outputStream = new FileOutputStream(this.dataDir + "/" + Configuration.getTableToTypesFile());
        writer = new OutputStreamWriter(outputStream);
        Set<String> tables = new HashSet<>();
        Iterator<Id> entityIdIter = ((EntityLinking) this.linker.linker()).uriIds();

        while (entityIdIter.hasNext())
        {
            List<String> entityTables = this.entityTableLink.find(entityIdIter.next());

            for (String t : entityTables)
            {
                if (tables.contains(t))
                    continue;

                tables.add(t);
                writer.write("<http://thetis.edao.eu/wikitables/" + t +
                        "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                        "<https://schema.org/Table> .\n");
            }
        }

        writer.flush();
        writer.close();
    }

    /**
     * Elapsed time of loading in nanoseconds
     * @return Elapsed time of loading
     */
    public long elapsedTime()
    {
        return this.elapsed;
    }

    /**
     * Number of successfully loaded tables
     * @return Number of successfully loaded tables
     */
    public int loadedTables()
    {
        return (int) this.loadedTables.get();
    }

    public int cellsWithLinks()
    {
        return this.cellsWithLinks.get();
    }

    /**
     * Entity linker getter
     * @return Entity linker from link to entity URI
     */
    public EntityLinking getEntityLinker()
    {
        return (EntityLinking) this.linker.linker();
    }

    /**
     * Getter to Entity table
     * @return Loaded entity table
     */
    public EntityTable getEntityTable()
    {
        return (EntityTable) this.entityTable.index();
    }

    /**
     * Getter to entity-table linker
     * @return Loaded entity-table linker
     */
    public EntityTableLink getEntityTableLinker()
    {
        return (EntityTableLink) this.entityTableLink.index();
    }

    /**
     * Getter to HNSW index
     * @return HNSW index
     */
    public HNSW getHNSW()
    {
        return (HNSW) this.hnsw.index();
    }

    public long getApproximateEntityMentions()
    {
        return this.filter.approximateElementCount();
    }
}
