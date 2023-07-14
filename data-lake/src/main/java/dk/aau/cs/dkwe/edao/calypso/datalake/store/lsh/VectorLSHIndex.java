package dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EmbeddingsIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.PairNonComparable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Aggregator;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.ColumnAggregator;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.datalake.utilities.Utils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.random.RandomGenerator;

/**
 * LSH index of entity embeddings
 * Mapping from string entity to set of tables candidate entities by cosine similarity originate from
 */
public class VectorLSHIndex extends BucketIndex<Id, String> implements LSHIndex<String, String>, Serializable
{
    private Set<List<Double>> projections;
    private final int bandSize;
    private final boolean aggregateColumns;
    private RandomGenerator randomGen;
    private final transient int threads;
    private transient final Object lock = new Object();
    private transient EntityLinking linker;
    private final HashFunction hash;
    private final transient Cache<Id, List<Integer>> cache;
    private transient EmbeddingsIndex<Id> embeddingsIdx;

    /**
     * @param bucketCount Number of LSH index buckets
     * @param projections Number of projections, which determines hash size
     * @param tables Set of tables containing entities to be loaded
     * @param hash Hash function applied to bit vector representations of entities
     */
    public VectorLSHIndex(int bucketGroups, int bucketCount, int projections, int bandSize,
                          Set<PairNonComparable<String, Table<String>>> tables, int threads, RandomGenerator randomGenerator,
                          EntityLinking linker, EmbeddingsIndex<Id> embeddings, HashFunction hash, boolean aggregateColumns)
    {
        super(bucketGroups, bucketCount);
        this.bandSize = bandSize;
        this.randomGen = randomGenerator;
        this.threads = threads;
        this.linker = linker;
        this.embeddingsIdx = embeddings;
        this.hash = hash;
        this.aggregateColumns = aggregateColumns;
        this.cache = CacheBuilder.newBuilder().maximumSize(500).build();
        load(tables, projections);
    }

    public void useEntityLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    public void useEmbeddingIndex(EmbeddingsIndex<Id> embeddingsIdx)
    {
        this.embeddingsIdx = embeddingsIdx;
    }

    private void load(Set<PairNonComparable<String, Table<String>>> tables, int projections)
    {
        ExecutorService executor = Executors.newFixedThreadPool(this.threads);
        List<Future<?>> futures = new ArrayList<>(tables.size());

        if (tables.isEmpty())
        {
            throw new RuntimeException("No tables to load LSH index of embeddings");
        }

        int dimension = this.embeddingsIdx.getDimension();

        if (dimension == -1)
        {
            throw new RuntimeException("No embeddings exists for table entities");
        }

        this.projections = createProjections(projections, dimension, this.randomGen);

        for (PairNonComparable<String, Table<String>> table : tables)
        {
            futures.add(executor.submit(() -> loadTable(table)));
        }

        try
        {
            for (Future<?> f : futures)
            {
                f.get();
            }
        }

        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException("Error in multi-threaded loading of LSH index: " + e.getMessage());
        }
    }

    private void loadTable(PairNonComparable<String, Table<String>> table)
    {
        String tableName = table.first();
        Table<String> t = table.second();
        int rows = t.rowCount();

        if (this.aggregateColumns)
        {
            loadByColumns(tableName, t);
            return;
        }

        for (int row = 0; row < rows; row++)
        {
            for (int column = 0; column < t.getRow(row).size(); column++)
            {
                String entity = t.getRow(row).get(column);

                if (entity == null)
                {
                    continue;
                }

                List<Double> embedding;
                Id entityId = this.linker.uriLookup(entity);
                List<Integer> keys;

                if ((keys = this.cache.getIfPresent(entityId)) != null)
                {
                    insertEntity(entityId, keys, tableName);
                    continue;
                }

                synchronized (this.lock)
                {
                    embedding = this.embeddingsIdx.find(entityId);

                    if (embedding == null)
                    {
                        continue;
                    }
                }

                List<Integer> bitVector = bitVector(embedding);
                keys = createKeys(this.projections.size(), this.bandSize, bitVector, groupSize(), this.hash);
                this.cache.put(entityId, keys);
                insertEntity(entityId, keys, tableName);
            }
        }
    }

    private void loadByColumns(String tableName, Table<String> table)
    {
        Aggregator<String> aggregator = new ColumnAggregator<>(table);
        List<List<Double>> aggregatedColumns =
                aggregator.aggregate(entity -> {
                    Id entityId = this.linker.uriLookup(entity);

                    if (entityId == null)
                    {
                        return null;
                    }

                    return this.embeddingsIdx.find(entityId);
                }, coll -> Utils.averageVector(new ArrayList<>(coll)));

        for (List<Double> averageEmbedding : aggregatedColumns)
        {
            List<Integer> bitVector = bitVector(averageEmbedding);
            List<Integer> keys = createKeys(this.projections.size(), this.bandSize, bitVector, groupSize(), this.hash);
            insertEntity(Id.any(), keys, tableName);
        }
    }

    private void insertEntity(Id entityId, List<Integer> keys, String tableName)
    {
        for (int group = 0; group < keys.size(); group++)
        {
            synchronized (this.lock)
            {
                add(group, keys.get(group), entityId, tableName);
            }
        }
    }

    private static Set<List<Double>> createProjections(int num, int dimension, RandomGenerator r)
    {
        Set<List<Double>> projections = new HashSet<>();
        double min = -1.0, max = 1.0;

        for (int i = 0; i < num; i++)
        {
            List<Double> projection = new ArrayList<>(dimension);

            for (int dim = 0; dim < dimension; dim++)
            {
                projection.add(min + (max - min) * r.nextDouble());
            }

            projections.add(projection);
        }

        return projections;
    }

    private static double dot(List<Double> v1, List<Double> v2)
    {
        if (v1.size() != v2.size())
        {
            throw new IllegalArgumentException("Vectors are not of the same dimension");
        }

        double product = 0;

        for (int i = 0; i < v1.size(); i++)
        {
            product += v1.get(i) * v2.get(i);
        }

        return product;
    }

    private List<Integer> bitVector(List<Double> vector)
    {
        List<Integer> bitVector = new ArrayList<>(this.projections.size());

        for (List<Double> projection : this.projections)
        {
            double dotProduct = dot(projection, vector);
            bitVector.add(dotProduct > 0 ? 1 : 0);
        }

        return bitVector;
    }

    @Override
    public boolean insert(String entity, String table)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("Missing EntityLinker object");
        }

        else if (this.embeddingsIdx == null)
        {
            throw new RuntimeException("Missing EmbeddingsIndex object");
        }

        Id entityId = this.linker.uriLookup(entity);

        if (entityId == null)
        {
            throw new RuntimeException("Entity does not exist in specified EntityLinker object");
        }

        List<Double> embedding = this.embeddingsIdx.find(entityId);

        if (embedding == null)
        {
            return false;
        }

        List<Integer> bitVector = bitVector(embedding);
        List<Integer> keys = createKeys(this.projections.size(), this.bandSize, bitVector, groupSize(), this.hash);
        insertEntity(entityId, keys, table);

        return true;
    }

    @Override
    public Set<String> search(String entity)
    {
        return search(entity, 1);
    }

    @Override
    public Set<String> search(String entity, int vote)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("Missing EntityLinker object");
        }

        else if (this.embeddingsIdx == null)
        {
            throw new RuntimeException("Missing EmbeddingsIndex object");
        }

        Id entityId = this.linker.uriLookup(entity);

        if (entityId == null)
        {
            return new HashSet<>();
        }

        List<Double> embedding = this.embeddingsIdx.find(entityId);

        if (embedding == null)
        {
            return new HashSet<>();
        }

        List<Integer> searchBitVector = bitVector(embedding);
        List<Integer> keys = createKeys(this.projections.size(), this.bandSize, searchBitVector, groupSize(), this.hash);
        return super.search(keys, vote);
    }

    @Override
    public Set<String> agggregatedSearch(String ... keys)
    {
        return agggregatedSearch(1, keys);
    }

    @Override
    public Set<String> agggregatedSearch(int vote, String ... keys)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("Missing EntityLinker object");
        }

        else if (this.embeddingsIdx == null)
        {
            throw new RuntimeException("Missing EmbeddingsIndex object");
        }

        List<List<Double>> keyEmbeddings = new ArrayList<>();

        for (String key : keys)
        {
            Id id = this.linker.uriLookup(key);

            if (id == null)
            {
                continue;
            }

            List<Double> embedding = this.embeddingsIdx.find(id);

            if (embedding != null)
            {
                keyEmbeddings.add(embedding);
            }
        }

        List<Double> averageEmbedding = Utils.averageVector(keyEmbeddings);
        List<Integer> bitVector = bitVector(averageEmbedding);
        List<Integer> bandKeys = createKeys(this.projections.size(), this.bandSize, bitVector, groupSize(), this.hash);
        return super.search(bandKeys, vote);
    }
}
