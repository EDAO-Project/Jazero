package dk.aau.cs.dkwe.edao.calypso.datalake.loader;

import dk.aau.cs.dkwe.edao.calypso.datalake.store.EmbeddingsIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.TypesLSHIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh.VectorLSHIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class IndexReader implements IndexIO
{
    private final boolean multithreaded, logProgress;
    private final File indexDir;

    // Indexes
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private EmbeddingsIndex<String> embeddingsIdx;
    private TypesLSHIndex typesLSH;
    private VectorLSHIndex vectorsLSH;
    private static final int INDEX_COUNT = 5;

    public IndexReader(File indexDir, boolean isMultithreaded, boolean logProgress)
    {
        if (!indexDir.isDirectory())
            throw new IllegalArgumentException("'" + indexDir + "' is not a directory");

        else if (!indexDir.exists())
            throw new IllegalArgumentException("'" + indexDir + "' does not exist");

        this.indexDir = indexDir;
        this.multithreaded = isMultithreaded;
        this.logProgress = logProgress;
    }

    /**
     * Reads indexes from disk
     * @throws IOException thrown on IO error
     */
    @Override
    public void performIO() throws IOException
    {
        ExecutorService threadPoolService = Executors.newFixedThreadPool(this.multithreaded ? INDEX_COUNT : 1);
        Future<?> f1 = threadPoolService.submit(this::loadEntityLinker);
        Future<?> f2 = threadPoolService.submit(this::loadEntityTable);
        Future<?> f3 = threadPoolService.submit(this::loadEntityTableLink);
        Future<?> f4 = threadPoolService.submit(this::loadEmbeddingsIndex);
        Future<?> f5 = threadPoolService.submit(this::loadLSHIndexes);
        int completed = -1;

        while (!f1.isDone() || !f2.isDone() || !f3.isDone() || !f4.isDone() || !f5.isDone())
        {
            int tmpCompleted = (f1.isDone() ? 1 : 0) + (f2.isDone() ? 1 : 0) + (f3.isDone() ? 1 : 0) +
                    (f4.isDone() ? 1 : 0) + (f5.isDone() ? 1 : 0);

            if (tmpCompleted != completed)
            {
                completed = tmpCompleted;
                Logger.log(Logger.Level.INFO, "Loaded indexes: " + completed + "/" + INDEX_COUNT);
            }
        }

        Logger.log(Logger.Level.INFO, "Loaded indexes: " + INDEX_COUNT + "/" + INDEX_COUNT);

        try
        {
            f1.get();
            f2.get();
            f3.get();
            f4.get();
            f5.get();
        }

        catch (InterruptedException | ExecutionException e)
        {
            Logger.log(Logger.Level.ERROR, "Failed loading an index:");
            Logger.log(Logger.Level.ERROR, e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    private void loadEntityLinker()
    {
        this.linker = (EntityLinking) readIndex(this.indexDir + "/" + Configuration.getEntityLinkerFile());
    }

    private void loadEntityTable()
    {
        this.entityTable = (EntityTable) readIndex(this.indexDir + "/" + Configuration.getEntityTableFile());
    }

    private void loadEntityTableLink()
    {
        this.entityTableLink = (EntityTableLink) readIndex(this.indexDir + "/" + Configuration.getEntityToTablesFile());
    }

    private void loadEmbeddingsIndex()
    {
        this.embeddingsIdx = (EmbeddingsIndex<String>) readIndex(this.indexDir + "/" + Configuration.getEmbeddingsIndexFile());
    }

    private void loadLSHIndexes()
    {
        this.typesLSH = (TypesLSHIndex) readIndex(this.indexDir + "/" + Configuration.getTypesLSHIndexFile());
        this.vectorsLSH = (VectorLSHIndex) readIndex(this.indexDir + "/" + Configuration.getEmbeddingsLSHFile());
    }

    private Object readIndex(String file)
    {
        try (ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file)))
        {
            return stream.readObject();
        }

        catch (OptionalDataException e)
        {
            if (e.eof)
                Logger.log(Logger.Level.ERROR, "EOF reached earlier than expected when reading index file: " + file);

            else
                Logger.log(Logger.Level.ERROR, "Index file stream contains primitive data: " + file);

            throw new RuntimeException(e.getMessage());
        }

        catch (IOException | ClassNotFoundException e)
        {
            Logger.log(Logger.Level.ERROR, "IO error when reading index");
            throw new RuntimeException(e.getMessage());
        }
    }

    public EntityLinking getLinker()
    {
        return this.linker;
    }

    public EntityTable getEntityTable()
    {
        return this.entityTable;
    }

    public EntityTableLink getEntityTableLink()
    {
        return this.entityTableLink;
    }

    public EmbeddingsIndex<String> getEmbeddingsIndex()
    {
        return this.embeddingsIdx;
    }

    public TypesLSHIndex getTypesLSH()
    {
        return this.typesLSH;
    }

    public VectorLSHIndex getVectorsLSH()
    {
        return this.vectorsLSH;
    }
}
