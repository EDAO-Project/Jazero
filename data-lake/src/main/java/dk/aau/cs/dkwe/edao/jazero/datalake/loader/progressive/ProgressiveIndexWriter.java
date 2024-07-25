package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import dk.aau.cs.dkwe.edao.jazero.datalake.connector.DBDriverBatch;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.ELService;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.IndexWriter;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.StorageHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

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
    private int largestTable = 0;
    private double maxPriority = -1.0;
    private final HashSet<String> insertedIds = new HashSet<>();

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
                Table.Row<String> indexedRow = (Table.Row<String>) item.index();

                if (indexedRow != null)
                {
                    int tableSize = item.getIndexable().rowCount();
                    double decrement = (double) this.largestTable / tableSize;
                    item.setPriority(item.getPriority() - decrement);
                    this.largestTable = Math.max(this.largestTable, tableSize);
                    this.maxPriority = Math.max(this.maxPriority, item.getPriority() - decrement);

                    synchronized (super.lock)
                    {
                        if (!item.isIndexed())
                        {
                            this.scheduler.addIndexTable(item);
                        }

                        else
                        {
                            Logger.log(Logger.Level.INFO, "Fully indexed " + super.loadedTables.incrementAndGet() + "/" + this.corpusSize + " tables");
                        }

                        if (!this.insertedIds.contains(item.getId()))
                        {
                            super.storage.insert(((IndexTable) item).getFilePath().toFile());
                            this.insertedIds.add(item.getId());
                        }
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

    private void finalizeIndexing()
    {
        try
        {
            Logger.log(Logger.Level.INFO, "Collecting IDF weights...");
            loadIDFs();

            Logger.log(Logger.Level.INFO, "Writing indexes on disk...");
            synchronizeIndexes(super.indexDir, super.linker.linker(), super.entityTable.index(), super.entityTableLink.index(), super.hnsw);
            genNeo4jTableMappings();
            Logger.log(Logger.Level.INFO, "Progressive indexing has completed");
        }

        catch (IOException e)
        {
            throw new RuntimeException("Exception during progressive indexing: " + e.getMessage());
        }
    }

    private void indexRow(String id, int row, Table.Row<String> rowToindex)
    {
        int entities = rowToindex.size();
        List<String> indexedRow = new ArrayList<>();

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
                indexedRow.add(uri);
            }
        }

        if (!this.indexedTables.containsKey(id))
        {
            this.indexedTables.put(id, new DynamicTable<>());
        }

        this.indexedTables.get(id).addRow(new Table.Row<>(indexedRow));
    }

    /**
     * Adds a new table to the scheduler and assigns the table the currently highest priority
     * @param tablePath Path to the table file
     * @return True if the table was added to the scheduler and false if the table file could not be parsed
     */
    @Override
    public boolean addTable(Path tablePath)
    {
        IndexTable tableToIndex = new IndexTable(tablePath, this.maxPriority, this::indexRow);
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
    public synchronized void pauseIndexing()
    {
        this.isPaused = true;
        this.isRunning = false;
    }

    /**
     * Continues indexing after being paused
     */
    @Override
    public synchronized void continueIndexing()
    {
        this.isPaused = false;
        this.isRunning = true;
        this.schedulerThread.interrupt();
    }

    /**
     * Checks whether the progressive indexing is still running
     * @return True if progressive indexing is currently running
     */
    public boolean isRunning()
    {
        return this.isRunning;
    }

    /**
     * Allows updating indexables externally
     * @param id ID of indexable to update
     * @param update Procedure for updating the identified indexable
     */
    public void updatePriority(String id, Consumer<Indexable> update)
    {
        this.scheduler.update(id, update);
    }
}
