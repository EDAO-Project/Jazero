package dk.aau.cs.dkwe.edao.jazero.datalake.store.hnsw;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.Index;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Embedding;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Hierarchical Navigable Small World (HNSW) index of KG entities by their dense representation
 * The user specifies a generator for the dense representation given an entity
 */
public class HNSW implements Index<String, Set<String>>
{
    private transient Function<Entity, Embedding> embeddingGen;
    private cloud.unum.usearch.Index hnsw;
    private int embeddingsDim, k;
    private long capacity;
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private String indexPath;

    public HNSW(Function<Entity, Embedding> embeddingGenerator, int embeddingsDimension, long capacity, int neighborhoodSize,
                EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, String indexPath)
    {
        this.embeddingGen = embeddingGenerator;
        this.embeddingsDim = embeddingsDimension;
        this.capacity = capacity;
        this.k = neighborhoodSize;
        this.hnsw = new cloud.unum.usearch.Index.Config().metric("cos").dimensions(embeddingsDimension).build();
        this.linker = linker;
        this.entityTable = entityTable;
        this.entityTableLink = entityTableLink;
        this.indexPath = indexPath;
        this.hnsw.reserve(capacity);
    }

    private HNSW() {}

    public void setLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    public void setEntityTable(EntityTable entityTable)
    {
        this.entityTable = entityTable;
    }

    public void setEntityTableLink(EntityTableLink entityTableLink)
    {
        this.entityTableLink = entityTableLink;
    }

    public void setEmbeddingGenerator(Function<Entity, Embedding> embeddingGenerator)
    {
        this.embeddingGen = embeddingGenerator;
    }

    public int getEmbeddingsDimension()
    {
        return this.embeddingsDim;
    }

    public long getCapacity()
    {
        return this.capacity;
    }

    public int getNeighborhoodSize()
    {
        return this.k;
    }

    public String getIndexPath()
    {
        return this.indexPath;
    }

    private static float[] toPrimitiveEmbeddings(Embedding embedding)
    {
        float[] embeddingsArray = new float[embedding.getDimension()];
        int i = 0;

        for (double e : embedding.toList())
        {
            embeddingsArray[i++] = (float) e;
        }

        return embeddingsArray;
    }

    public void setCapacity(long capacity)
    {
        this.capacity = capacity;
        this.hnsw.reserve(this.capacity);
    }

    public void setK(int k)
    {
        this.k = k;
    }

    /**
     * Inserts an entry into the HSNW index that maps an entity to a set of tables
     * If the mapping already exists, the tables will be added to the co-domain of the mapping
     * @param key Entity to be inserted
     * @param tables Tables to be inserted into the index and mapped to by the given entity (ignore this parameter since we retrieve the tables from another index)
     */
    @Override
    public void insert(String key, Set<String> tables)
    {
        Id id = this.linker.uriLookup(key);

        if (id == null)
        {
            return;
        }

        Entity entity = this.entityTable.find(id);
        Embedding embedding = this.embeddingGen.apply(entity);
        float[] embeddingsArray = toPrimitiveEmbeddings(embedding);

        this.hnsw.add(id.id(), embeddingsArray);
    }

    /**
     * Removes the entity from the HNSW index
     * @param key Entity to be removed
     * @return True if the entity has an ID and thereby can be removed, otherwise false
     */
    @Override
    public boolean remove(String key)
    {
        Id id = this.linker.uriLookup(key);

        if (id == null)
        {
            return false;
        }

        this.hnsw.remove(id.id());
        return true;
    }

    /**
     * Finds tables containing K-nearest neighbors
     * @param key Query entity
     * @return Set of tables
     */
    @Override
    public Set<String> find(String key)
    {
        Id id = this.linker.uriLookup(key);

        if (id == null)
        {
            return Collections.emptySet();
        }

        Entity entity = this.entityTable.find(id);
        Embedding embedding = this.embeddingGen.apply(entity);
        float[] primitiveEmbedding = toPrimitiveEmbeddings(embedding);
        int[] ids = this.hnsw.search(primitiveEmbedding, this.k);
        Set<String> tables = new HashSet<>();

        for (int resultId : ids)
        {
            tables.addAll(this.entityTableLink.find(new Id(resultId)));
        }

        return tables;
    }

    /**
     * Approximately checks whether a given entity exists in the HNSW index
     * @param key Entity to check
     * @return True if the entity is in the top-K nearest neighbors
     */
    @Override
    public boolean contains(String key)
    {
        Id id = this.linker.uriLookup(key);

        if (id == null)
        {
            return false;
        }

        Entity entity = this.entityTable.find(id);
        Embedding embedding = this.embeddingGen.apply(entity);
        float[] primitiveEmbedding = toPrimitiveEmbeddings(embedding);
        int[] ids = this.hnsw.search(primitiveEmbedding, this.k);

        for (int resultId : ids)
        {
            if (this.entityTable.contains(new Id(resultId)))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Number of entities in the HNSW index
     * @return The size of the HNSW index in terms of number of stored entities
     */
    @Override
    public long size()
    {
        return this.hnsw.size();
    }

    /**
     * Clears the HNSW index
     */
    @Override
    public void clear()
    {
        this.hnsw = new cloud.unum.usearch.Index.Config().metric("cos").dimensions(this.embeddingsDim).build();
    }

    public void save()
    {
        this.hnsw.save(this.indexPath);
    }

    public void load()
    {
        this.hnsw = new cloud.unum.usearch.Index.Config().metric("cos").dimensions(this.embeddingsDim).build();
        this.hnsw.reserve(this.capacity);
        this.hnsw.load(this.indexPath);
    }
}
