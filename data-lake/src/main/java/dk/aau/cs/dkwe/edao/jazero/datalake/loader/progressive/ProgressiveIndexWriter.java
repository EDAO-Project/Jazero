package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import dk.aau.cs.dkwe.edao.jazero.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.IndexWriter;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh.SetLSHIndex;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh.VectorLSHIndex;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.PairNonComparable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.StorageHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Main class for progressively indexing of tables in an adaptive fashion
 * It guarantees that the indexes will converge, but it will also adapt its indexing of table rows
 */
public class ProgressiveIndexWriter extends IndexWriter implements ProgressiveIndexIO
{
    private final Runnable cleanupProcess;
    private Thread schedulerThread;
    private boolean isRunning = false, isPaused = false;
    private final Scheduler scheduler;
    private final Map<String, Table<String>> indexedTables = new HashMap<>();
    private final int corpusSize;
    private int indexedRows = 0;
    private static final int PRE_LOAD_ROWS_THRES = 2500;

    public ProgressiveIndexWriter(List<Path> files, File indexPath, File dataOutputPath, StorageHandler.StorageType storageType,
                                  KGService kgService, ELService elService, DBDriverBatch<List<Double>, String> embeddingStore,
                                  int threads, String wikiPrefix, String uriPrefix, Scheduler scheduler, Runnable cleanup)
    {
        super(files, indexPath, dataOutputPath, storageType, kgService, elService, embeddingStore, threads, wikiPrefix, uriPrefix);
        this.scheduler = scheduler;
        this.cleanupProcess = cleanup;
        this.corpusSize = files.size();

        for (Path path : files)
        {
            IndexTable it = new IndexTable(path, this::indexRow);
            this.scheduler.addIndexTable(it);
        }
    }

    /**
     * Starts thread to progressively index tables
     */
    @Override
    public void performIO()
    {
        Runnable indexing = () -> {
            while (this.scheduler.hasNext())
            {
                while (this.isPaused)
                {
                    try
                    {
                        Thread.sleep(10000);
                    }

                    catch (InterruptedException ignore) {}
                }

                Indexable item = this.scheduler.next();
                Table.Row<String> indexedRow = ((Table<String>) item.getIndexable()).getRow(0);
                item.index();
                this.indexedRows++;
                // TODO: Change the priority here

                if (this.indexedRows >= PRE_LOAD_ROWS_THRES)
                {
                    Logger.log(Logger.Level.INFO, "Starting to load LSH indexes");
                    preLoadLSH();
                }

                if (!item.isIndexed())
                {
                    if (!this.indexedTables.containsKey(item.getId()))
                    {
                        this.indexedTables.put(item.getId(), new DynamicTable<>());
                    }

                    this.scheduler.addIndexTable(item);
                    this.indexedTables.get(item.getId()).addRow(indexedRow);
                }

                else
                {
                    synchronized (super.lock)
                    {
                        super.storage.insert(((IndexTable) item).getFilePath().toFile());
                        Logger.log(Logger.Level.INFO, "Fully indexed " + super.loadedTables.get() + "/" + this.corpusSize + " tables");
                    }
                }
            }

            this.cleanupProcess.run();
            this.isRunning = false;
            finalizeIndexing();
        };
        this.schedulerThread = new Thread(indexing);
        this.schedulerThread.start();
        this.isRunning = true;
    }

    private void preLoadLSH()
    {
        int permutations = Configuration.getPermutationVectors(), bandSize = Configuration.getBandSize();
        int bucketGroups = permutations / bandSize, bucketsPerGroup = (int) Math.pow(2, bandSize);

        if (permutations % bandSize != 0)
        {
            throw new IllegalArgumentException("Number of permutation/projection vectors is not divisible by band size");
        }

        Set<PairNonComparable<String, Table<String>>> tables = new HashSet<>();
        this.indexedTables.forEach((tableId, table) -> tables.add(new PairNonComparable<>(tableId, table)));

        super.typesLSH = new SetLSHIndex(permutations, SetLSHIndex.EntitySet.TYPES, bandSize, 2, tables, HASH_FUNCTION_NUMERIC,
                bucketGroups, bucketsPerGroup, 1, new Random(0), getEntityLinker(), getEntityTable(), false);
        super.embeddingsLSH = new VectorLSHIndex(bucketGroups, bucketsPerGroup, permutations, bandSize, tables, 1,
                new Random(0), getEntityLinker(), getEntityTable(), HASH_FUNCTION_BOOLEAN, false);
    }

    private void finalizeIndexing()
    {
        try
        {
            Logger.log(Logger.Level.INFO, "Progressive indexing has completed");
            Logger.log(Logger.Level.INFO, "Collecting IDF weights...");
            loadIDFs();

            Logger.log(Logger.Level.INFO, "Writing indexes on disk...");
            synchronizeIndexes(super.indexDir, super.linker.linker(), super.entityTable.index(), super.entityTableLink.index(),
                    super.typesLSH, super.embeddingsLSH);
            genNeo4jTableMappings();
        }

        catch (IOException e)
        {
            throw new RuntimeException("Exception during progressive indexing: " + e.getMessage());
        }
    }

    private void indexRow(String id, int row, Table.Row<String> rowToindex)
    {
        int entities = rowToindex.size();

        for (int i = 0; i < entities; i++)
        {
            String tableEntity = rowToindex.get(i);
            String uri = indexEntity(tableEntity);

            if (uri != null)
            {
                Id entityId = ((EntityLinking) super.linker.linker()).uriLookup(uri);
                super.cellsWithLinks.incrementAndGet();
                ((EntityTableLink) super.entityTableLink.index()).addLocation(entityId, id, List.of(new Pair<>(row, i)));
                super.filter.put(uri);

                if (this.indexedRows > PRE_LOAD_ROWS_THRES)
                {
                    this.typesLSH.insert(uri, id);
                    this.embeddingsLSH.insert(uri, id);
                }
            }
        }
    }

    /**
     * Adds a new table to the scheduler
     * @param tablePath Path to the table file
     * @return True if the table was added to the scheduler and false if the table file could not be parsed
     */
    @Override
    public boolean addTable(Path tablePath)
    {
        // TODO: Compute its correct priority
        IndexTable tableToIndex = new IndexTable(tablePath, 0, this::indexRow);
        this.scheduler.addIndexTable(tableToIndex);

        return true;
    }

    /**
     * Waits until the scheduler has completed all of its tasks
     */
    @Override
    public void waitForCompletion()
    {
        try
        {
            this.schedulerThread.join();
            this.isRunning = false;
        }

        catch (InterruptedException ignored) {}
    }

    /**
     * Pauses indexing
     */
    @Override
    public void pauseIndexing()
    {
        this.isPaused = true;
    }

    /**
     * Continues indexing after being paused
     */
    @Override
    public void continueIndexing()
    {
        this.isPaused = false;
        this.schedulerThread.interrupt();
    }

    /**
     * Triggers an event that updates the priorities of indexable tables
     * @param event Event that describes the type of event and the involved tables
     */
    @Override
    public void triggerEvent(Event event)
    {
        switch (event.type())
        {
            case INSERTION:
        }
    }

    public boolean isRunning()
    {
        return this.isRunning;
    }
}
