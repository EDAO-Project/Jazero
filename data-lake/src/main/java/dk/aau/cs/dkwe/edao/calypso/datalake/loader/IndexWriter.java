package dk.aau.cs.dkwe.edao.calypso.datalake.loader;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.ParsingException;
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.*;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.HashFunction;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.TypesLSHIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.VectorLSHIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.PairNonComparable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.FileLogger;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.calypso.datalake.tables.JsonTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.utilities.Utils;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.StorageHandler;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IndexWriter implements IndexIO
{
    private final List<Path> files;
    private final File indexDir, dataDir;
    private final StorageHandler storage;
    private final int threads;
    private final AtomicInteger loadedTables = new AtomicInteger(0),
            cellsWithLinks = new AtomicInteger(0), tableStatsCollected = new AtomicInteger(0);
    private final Object lock = new Object();
    private long elapsed = -1;
    private final Map<Integer, Integer> cellToNumLinksFrequency = Collections.synchronizedMap(new HashMap<>());
    private final KGService kg;
    private final ELService el;
    private final SynchronizedLinker<String, String> linker;
    private final  SynchronizedIndex<Id, Entity> entityTable;
    private final SynchronizedIndex<Id, List<String>> entityTableLink;
    private final SynchronizedIndex<Id, List<Double>> embeddingsIdx;
    private TypesLSHIndex typesLSH;
    private VectorLSHIndex embeddingsLSH;
    private final DBDriverBatch<List<Double>, String> embeddingsDB;
    private final BloomFilter<String> filter = BloomFilter.create(
            Funnels.stringFunnel(Charset.defaultCharset()),
            5_000_000,
            0.01);
    private final Map<String, Stats> tableStats = new TreeMap<>();
    private final Set<PairNonComparable<String, Table<String>>> tableEntities = Collections.synchronizedSet(new HashSet<>());

    private static final List<String> DISALLOWED_ENTITY_TYPES =
            Arrays.asList("http://www.w3.org/2002/07/owl#Thing", "http://www.wikidata.org/entity/Q5");
    private static final String STATS_DIR = "statistics/";

    private static final HashFunction HASH_FUNCTION_NUMERIC = (obj, num) -> {
        List<Integer> sig = (List<Integer>) obj;
        int sum1 = 0, sum2 = 0, size = sig.size();

        for (int i = 0; i < size; i++)
        {
            sum1 = (sum1 + sig.get(i)) % 255;
            sum2 = (sum2 + sum1) % 255;
        }

        return ((sum2 << 8) | sum1) % num;
    };
    private static final HashFunction HASH_FUNCTION_BOOLEAN = (obj, num) -> {
        List<Integer> vector = (List<Integer>) obj;
        int sum = 0, dim = vector.size();

        for (int i = 0; i < dim; i++)
        {
            sum += vector.get(i) * Math.pow(2, i);
        }

        return sum % num;
    };

    public IndexWriter(List<Path> files, File indexPath, File dataOutputPath, StorageHandler.StorageType storageType, KGService kgService,
                       ELService elService, DBDriverBatch<List<Double>, String> embeddingStore, int threads, String wikiPrefix, String uriPrefix)
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
        this.embeddingsIdx = SynchronizedIndex.wrap(new EmbeddingsIndex<>());
        this.entityTable = SynchronizedIndex.wrap(new EntityTable());
        this.entityTableLink = SynchronizedIndex.wrap(new EntityTableLink());
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
        long startTime = System.nanoTime();

        for (int i = 0; i < size; i++)
        {
            if (load(this.files.get(i)))
                this.loadedTables.incrementAndGet();

            if (this.loadedTables.get() % 100 == 0)
                Logger.log(Logger.Level.INFO, "Processed " + (i + 1) + "/" + size + " files...");
        }

        Logger.log(Logger.Level.INFO, "Collecting IDF weights...");
        loadIDFs();

        Logger.log(Logger.Level.INFO, "Building LSH indexes");
        loadLSHIndexes();

        Logger.log(Logger.Level.INFO, "Writing indexes and stats on disk...");
        writeStats();
        this.tableStats.clear();    // Save memory before writing index objects to disk
        flushToDisk();

        this.elapsed = System.nanoTime() - startTime;
        Logger.log(Logger.Level.INFO, "Done");
        Logger.log(Logger.Level.INFO, "A total of " + this.loadedTables.get() + " tables were loaded");
        Logger.log(Logger.Level.INFO, "Elapsed time: " + this.elapsed / (1e9) + " seconds");
        Logger.log(Logger.Level.INFO, "Computing IDF weights...");
    }

    private void loadLSHIndexes()
    {
        int permutations = Configuration.getPermutationVectors(), bandSize = Configuration.getBandSize();
        int bucketGroups = permutations / bandSize, bucketsPerGroup = (int) Math.pow(2, bandSize);

        if (permutations % bandSize != 0)
        {
            throw new IllegalArgumentException("Number of permutation/projection vectors is not divisible by band size");
        }

        Logger.log(Logger.Level.INFO, "Loaded LSH index 0/2");
        this.typesLSH = new TypesLSHIndex(permutations, bandSize, 2,
                this.tableEntities, HASH_FUNCTION_NUMERIC, bucketGroups, bucketsPerGroup, this.threads, getEntityLinker(),
                getEntityTable(), false);

        Logger.log(Logger.Level.INFO, "Loaded LSH index 1/2");

        this.embeddingsLSH = new VectorLSHIndex(bucketGroups, bucketsPerGroup, permutations, bandSize, this.tableEntities,
                this.threads, getEntityLinker(), getEmbeddingsIndex(), HASH_FUNCTION_BOOLEAN, false);
        Logger.log(Logger.Level.INFO, "Loaded LSH index 2/2");
    }

    private boolean load(Path tablePath)
    {
        JsonTable table = parseTable(tablePath);

        if (table == null ||  table._id == null || table.rows == null)
            return false;

        String tableName = tablePath.getFileName().toString();
        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();  // Maps a cell specified by RowNumber, ColumnNumber to the list of entities it matches to
        Table<String> inputEntities = new DynamicTable<>();
        Set<String> entities = new HashSet<>(); // The set of entities corresponding to this filename/table
        int row = 0;

        for (List<JsonTable.TableCell> tableRow : table.rows)
        {
            int column = 0;
            List<String> inputRow = new ArrayList<>(tableRow.size());

            for (JsonTable.TableCell cell : tableRow)
            {
                this.cellsWithLinks.incrementAndGet();
                List<String> matchesUris = new ArrayList<>();
                String cellText = cell.text;
                String uri = this.linker.mapTo(cellText);

                if (uri == null)
                {
                    uri = this.el.link(cellText);

                    if (uri != null)
                    {
                        List<String> entityTypes = this.kg.searchTypes(uri);
                        matchesUris.add(uri);
                        this.linker.addMapping(cellText, uri);

                        for (String type : DISALLOWED_ENTITY_TYPES)
                        {
                            entityTypes.remove(type);
                        }

                        Id entityId = ((EntityLinking) this.linker.linker()).uriLookup(uri);
                        List<Double> embeddings = this.embeddingsDB.select(uri.replace("'", "''"));
                        this.entityTable.insert(entityId,
                                new Entity(uri, entityTypes.stream().map(Type::new).collect(Collectors.toList())));

                        if (embeddings != null)
                        {
                            this.embeddingsIdx.insert(entityId, embeddings);
                        }
                    }
                }

                inputRow.add(uri);

                if (uri != null)
                {
                    Id entityId = ((EntityLinking) this.linker.linker()).uriLookup(uri);
                    Pair<Integer, Integer> location = new Pair<>(row, column);
                    ((EntityTableLink) this.entityTableLink.index()).
                            addLocation(entityId, tableName, List.of(location));
                }

                if (!matchesUris.isEmpty())
                {
                    for (String entity : matchesUris)
                    {
                        this.filter.put(entity);
                        entities.add(entity);
                    }

                    entityMatches.put(new Pair<>(row, column), matchesUris);
                }

                column++;
            }

            inputEntities.addRow(new Table.Row<>(inputRow));
            row++;
        }

        this.tableEntities.add(new PairNonComparable<>(tableName, inputEntities));
        saveStats(table, FilenameUtils.removeExtension(tableName), entities.iterator(), entityMatches);
        this.storage.insert(tablePath.toFile());
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

        synchronized (this.lock)
        {
            this.tableStats.put(tableFileName, stats);
        }
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

    private void loadIDFs()
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
            this.entityTable.find(entityId).setIDF(idf);
        }
    }

    private void loadTypeIDFs()
    {
        Map<Type, Integer> entityTypeFrequency = new HashMap<>();
        Iterator<Id> idIterator = ((EntityLinking) this.linker.linker()).uriIds();

        while (idIterator.hasNext())
        {
            Id id = idIterator.next();
            List<Type> entityTypes = this.entityTable.find(id).getTypes();

            for (Type t : entityTypes)
            {
                if (entityTypeFrequency.containsKey(t))
                    entityTypeFrequency.put(t, entityTypeFrequency.get(t) + 1);

                else
                    entityTypeFrequency.put(t, 1);
            }
        }

        int totalEntityCount = this.entityTable.size();
        idIterator = ((EntityLinking) this.linker.linker()).uriIds();

        while (idIterator.hasNext())
        {
            Id id = idIterator.next();
            this.entityTable.find(id).getTypes().forEach(t -> {
                if (entityTypeFrequency.containsKey(t))
                {
                    double idf = Utils.log2((double) totalEntityCount / entityTypeFrequency.get(t));
                    t.setIdf(idf);
                }
            });
        }
    }

    private void flushToDisk() throws IOException
    {
        // Entity linker
        ObjectOutputStream outputStream =
                new ObjectOutputStream(new FileOutputStream(this.indexDir + "/" + Configuration.getEntityLinkerFile()));
        outputStream.writeObject(this.linker.linker());
        outputStream.flush();
        outputStream.close();

        // Entity table
        outputStream = new ObjectOutputStream(new FileOutputStream(this.indexDir + "/" + Configuration.getEntityTableFile()));
        outputStream.writeObject(this.entityTable.index());
        outputStream.flush();
        outputStream.close();

        // Entity to tables inverted index
        outputStream = new ObjectOutputStream(new FileOutputStream(this.indexDir + "/" + Configuration.getEntityToTablesFile()));
        outputStream.writeObject(this.entityTableLink.index());
        outputStream.flush();
        outputStream.close();

        // Embeddings index
        outputStream = new ObjectOutputStream(new FileOutputStream(this.indexDir + "/" + Configuration.getEmbeddingsIndexFile()));
        outputStream.writeObject(this.embeddingsIdx.index());
        outputStream.flush();
        outputStream.close();

        // LSH of entity types
        outputStream = new ObjectOutputStream(new FileOutputStream(this.indexDir + "/" + Configuration.getTypesLSHIndexFile()));
        outputStream.writeObject(this.typesLSH);
        outputStream.flush();
        outputStream.close();

        // LSH of entity embeddings
        outputStream = new ObjectOutputStream(new FileOutputStream(this.indexDir + "/" + Configuration.getEmbeddingsLSHFile()));
        outputStream.writeObject(this.embeddingsLSH);
        outputStream.flush();
        outputStream.close();

        genNeo4jTableMappings();
    }

    private void genNeo4jTableMappings() throws IOException
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
        return this.loadedTables.get();
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
     * Getter to embeddings index
     * @return Loaded embeddings index
     */
    public EmbeddingsIndex<String> getEmbeddingsIndex()
    {
        return (EmbeddingsIndex<String>) this.embeddingsIdx.index();
    }

    /**
     * Getter to LSH index of entity types
     * @return Entity types-based LSH index
     */
    public TypesLSHIndex getTypesLSH()
    {
        return this.typesLSH;
    }

    /**
     * Getter to LSH index of entity embeddings
     * @return Entity embeddings-based LSH index
     */
    public VectorLSHIndex getEmbeddingsLSH()
    {
        return this.embeddingsLSH;
    }

    public long getApproximateEntityMentions()
    {
        return this.filter.approximateElementCount();
    }
}