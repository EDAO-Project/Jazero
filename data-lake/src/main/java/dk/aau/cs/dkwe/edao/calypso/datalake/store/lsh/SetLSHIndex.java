package dk.aau.cs.dkwe.edao.calypso.datalake.store.lsh;

import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.PairNonComparable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Aggregator;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.ColumnAggregator;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * BucketIndex key is RDF type and value is table ID
 */
public class SetLSHIndex extends BucketIndex<Id, String> implements LSHIndex<String, String>, Serializable
{
    public enum EntitySet
    {
        TYPES, PREDICATES
    }

    private EntitySet setType;
    private final int shingles, permutationVectors, bandSize;
    private List<List<Integer>> permutations;
    private final List<PairNonComparable<Id, List<Integer>>> signature;
    private Map<String, Integer> universeElements;
    private final HashFunction hash;
    private final transient int threads;
    private RandomGenerator randomGen;
    private transient final Object lock = new Object();
    private transient EntityLinking linker;
    private transient EntityTable entityTable;
    private final Map<Id, Integer> entityToSigIndex = new HashMap<>();
    private final boolean aggregateColumns;
    private Set<String> unimportantElements;
    private static final double UNIMPORTANT_TABLE_PERCENTAGE = 0.5;

    /**
     * @param permutationVectors Number of permutation vectors used to create min-hash signature (this determines the signature dimension for each entity)
     * @param tables Set of tables containing  entities to be loaded
     * @param hash A hash function to be applied on min-hash signature to compute bucket index
     * @param bucketCount Number of LSH buckets (this determines runtime and accuracy!)
     */
    public SetLSHIndex(int permutationVectors, EntitySet set, int bandSize, int shingleSize,
                       Set<PairNonComparable<String, Table<String>>> tables, HashFunction hash, int bucketGroups,
                       int bucketCount, int threads, RandomGenerator randomGenerator, EntityLinking linker,
                       EntityTable entityTable, boolean aggregateColumns)
    {
        super(bucketGroups, bucketCount);

        if (bandSize <= 0)
        {
            throw new IllegalArgumentException("Band size must be greater than 0");
        }

        else if (shingleSize <= 0)
        {
            throw new IllegalArgumentException("Shingle size must be greater than 0");
        }

        this.setType = set;
        this.shingles = shingleSize;
        this.permutationVectors = permutationVectors;
        this.signature = new ArrayList<>();
        this.bandSize = bandSize;
        this.hash = hash;
        this.threads = threads;
        this.randomGen = randomGenerator;
        this.linker = linker;
        this.entityTable = entityTable;
        this.aggregateColumns = aggregateColumns;

        Set<Table<String>> linkedTables = tables.stream().map(PairNonComparable::second).collect(Collectors.toSet());
        loadElements(linkedTables, linker);

        try
        {
            build(tables);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Could not initialize Neo4J connector: " + e.getMessage());
        }
    }

    public void useEntityLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    public void useEntityTable(EntityTable entityTable)
    {
        this.entityTable = entityTable;
    }

    private void loadElements(Set<Table<String>> linkedTables, EntityLinking linker)
    {
        int counter = 0;
        this.universeElements = new HashMap<>();
        Iterator<?> elements = this.setType == EntitySet.TYPES ? entityTable.allTypes() : entityTable.allPredicates();

        while (elements.hasNext())
        {
            String element = this.setType == EntitySet.TYPES ? ((Type) elements.next()).getType() : elements.next().toString();

            if (!this.universeElements.containsKey(element))
            {
                this.universeElements.put(element, counter++);
            }
        }

        this.unimportantElements = new ElementStats(this.entityTable, this.setType).popularByTable(UNIMPORTANT_TABLE_PERCENTAGE,
                linkedTables, linker);
    }

    /**
     * Instead of storing actual matrix, we only store the smallest index per entity
     * as this is all we need when computing the signature
     */
    private void build(Set<PairNonComparable<String, Table<String>>> tables) throws IOException
    {
        if (this.linker == null)
        {
            throw new RuntimeException("No EntityLinker object has been specified");
        }

        ExecutorService executor = Executors.newFixedThreadPool(this.threads);
        List<Future<?>> futures = new ArrayList<>(tables.size());
        int elementsDimension = this.universeElements.size();

        for (int i = 1; i < this.shingles; i++)
        {
            elementsDimension = concat(elementsDimension, this.universeElements.size());
        }

        this.permutations = createPermutations(this.permutationVectors, ++elementsDimension, this.randomGen);

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
        List<PairNonComparable<Id, Set<Integer>>> matrix = new ArrayList<>();
        Table<String> t = table.second();
        int rows = t.rowCount();

        if (this.aggregateColumns)
        {
            loadByColumns(tableName, t);
            return;
        }

        for (int row = 0; row < rows; row++)
        {
            int columns = t.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                String entity = t.getRow(row).get(column);

                if (entity == null)
                {
                    continue;
                }

                Id entityId = this.linker.uriLookup(entity);
                Set<Integer> entityBitVector = bitVector(entity);

                if (!entityBitVector.isEmpty())
                {
                    matrix.add(new PairNonComparable<>(entityId, entityBitVector));
                }
            }
        }

        synchronized (this.lock)
        {
            extendSignature(this.signature, matrix, this.permutations, this.entityToSigIndex);
        }

        insertIntoBuckets(matrix, tableName);
    }

    private void loadByColumns(String tableName, Table<String> table)
    {
        List<PairNonComparable<Id, Set<Integer>>> matrix = new ArrayList<>();
        Aggregator<String> aggregator = new ColumnAggregator<>(table);
        List<Set<String>> aggregatedColumns =
                aggregator.aggregate(this::elements,
                        coll -> {
                            Set<String> elements = new HashSet<>();
                            coll.forEach(elements::addAll);
                            return elements;
                        });

        for (Set<String> column : aggregatedColumns)
        {
            if (!column.isEmpty())
            {
                Set<Integer> bitVector = bitVector(column);

                if (!bitVector.isEmpty())
                {
                    matrix.add(new PairNonComparable<>(Id.alloc(), bitVector));
                }
            }
        }

        synchronized (this.lock)
        {
            extendSignature(this.signature, matrix, this.permutations, this.entityToSigIndex);
        }

        insertIntoBuckets(matrix, tableName);
    }

    private void insertIntoBuckets(List<PairNonComparable<Id, Set<Integer>>> matrix, String tableName)
    {
        Set<Integer> newSignatures = matrix.stream().map(e -> this.entityToSigIndex.get(e.first())).collect(Collectors.toSet());

        for (int entityIdx : newSignatures)
        {
            List<Integer> keys = createKeys(this.permutations.size(), this.bandSize,
                    this.signature.get(entityIdx).second(), groupSize(), this.hash);
            int keysCount = keys.size();
            Id entityId = this.signature.get(entityIdx).first();

            for (int group = 0; group < keysCount; group++)
            {
                synchronized (this.lock)
                {
                    add(group, keys.get(group), entityId, tableName);
                }
            }
        }
    }

    private Set<Integer> bitVector(String entity)
    {
        Set<String> elements = elements(entity);
        return bitVector(elements);
    }

    private Set<Integer> bitVector(Set<String> elements)
    {
        elements = elements.stream().filter(e -> !this.unimportantElements.contains(e) &&
                    this.universeElements.containsKey(e)).collect(Collectors.toSet());
        Set<List<String>> shingles = ElementShingles.shingles(elements, this.shingles);
        Set<Integer> indices = new HashSet<>();

        for (List<String> shingle : shingles)
        {
            List<Integer> shingleIds = new ArrayList<>(shingle.stream().map(s -> this.universeElements.get(s)).toList());
            shingleIds.sort(Comparator.comparingInt(v -> v));

            int concatenated = shingleIds.get(0);

            for (int i = 1; i < shingleIds.size(); i++)
            {
                concatenated = concat(concatenated, shingleIds.get(i));
            }

            indices.add(concatenated);
        }

        return indices;
    }

    private Set<String> elements(String entity)
    {
        Id id = this.linker.uriLookup(entity);

        if (id == null)
        {
            return new HashSet<>();
        }

        Entity e = this.entityTable.find(id);

        if (e == null)
        {
            return new HashSet<>();
        }

        return this.setType == EntitySet.TYPES ? e.getTypes().stream().map(Type::getType).collect(Collectors.toSet())
                : new HashSet<>(e.getPredicates());
    }

    private static List<List<Integer>> createPermutations(int vectors, int dimension, RandomGenerator random)
    {
        List<List<Integer>> permutations = new ArrayList<>();

        for (int i = 0; i < vectors; i++)
        {
            List<Integer> permutation = new ArrayList<>(dimension);
            List<Integer> indices = IntStream.range(0, dimension).boxed().collect(Collectors.toCollection(LinkedList::new));

            while (!indices.isEmpty())
            {
                int idx = random.nextInt(indices.size());
                permutation.add(indices.remove(idx));
            }

            permutations.add(permutation);
        }

        return permutations;
    }

    private static List<PairNonComparable<Id, List<Integer>>> extendSignature(List<PairNonComparable<Id, List<Integer>>> signature,
                                                                     List<PairNonComparable<Id, Set<Integer>>> entityMatrix,
                                                                     List<List<Integer>> permutations,
                                                                     Map<Id, Integer> entityToSigIdx)
    {
        for (PairNonComparable<Id, Set<Integer>> entity : entityMatrix)
        {
            List<Integer> entitySignature;
            Id entityId = entity.first();

            if (!entityToSigIdx.containsKey(entityId))
            {
                Set<Integer> bitVector = entity.second();

                if (bitVector.isEmpty())
                {
                    entitySignature = new ArrayList<>(Collections.nCopies(permutations.size(), 0));
                }

                else
                {
                    entitySignature = new ArrayList<>(permutations.size());

                    for (List<Integer> permutation : permutations)
                    {
                        int reArrangedMin = reArrangeMin(bitVector, permutation);
                        entitySignature.add(permutation.get(reArrangedMin));
                    }
                }

                signature.add(new PairNonComparable<>(entity.first(), entitySignature));
                entityToSigIdx.put(entityId, signature.size() - 1);
            }
        }

        return signature;
    }

    private static int reArrangeMin(Set<Integer> bitVector, List<Integer> permutation)
    {
        Iterator<Integer> iter = bitVector.iterator();
        int smallest = Integer.MAX_VALUE;

        while (iter.hasNext())
        {
            int idx = iter.next();
            int permuted = permutation.get(idx);

            if (permuted < smallest)
            {
                smallest = permuted;
            }
        }

        return smallest;
    }

    private int createOrGetSignature(String entity)
    {
        Id entityId = this.linker.uriLookup(entity);

        if (entityId == null)
        {
            throw new RuntimeException("Entity does not exist in EntityLinker object");
        }

        Set<Integer> entityBitVector = bitVector(entity);
        return createOrGetSignature(entityId, entityBitVector);
    }

    private int createOrGetSignature(Id entityId, Set<Integer> bitVector)
    {
        if (bitVector.isEmpty())
        {
            return -1;
        }

        extendSignature(this.signature, List.of(new PairNonComparable<>(entityId, bitVector)),
                this.permutations, this.entityToSigIndex);
        return this.entityToSigIndex.get(entityId);
    }

    /**
     * Insert single entity into LSH index
     * @param entity Entity to be inserted
     * @param table Table in which the entity exists
     * @return True if the entity could be inserted
     */
    @Override
    public boolean insert(String entity, String table)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("No EntityLinker object has been specified");
        }

        else if (this.entityTable == null)
        {
            throw new RuntimeException("No EntityTable object has been specified");
        }

        Id entityId = this.linker.uriLookup(entity);

        if (entityId == null)
        {
            throw new RuntimeException("Entity does not exist in EntityLinker object");
        }

        try
        {
            int entitySignature = createOrGetSignature(entity);

            if (entitySignature == -1)
            {
                return false;
            }

            List<Integer> bucketKeys = createKeys(this.permutations.size(), this.bandSize,
                    this.signature.get(entitySignature).second(), groupSize(), this.hash);

            for (int group = 0; group < bucketKeys.size(); group++)
            {
                synchronized (this.lock)
                {
                    add(group, bucketKeys.get(group), entityId, table);
                }
            }

            return true;
        }

        catch (Exception exc)
        {
            return false;
        }
    }

    @Override
    public Set<String> search(String entity)
    {
        return search(entity, 1);
    }

    /**
     * Finds buckets of similar entities and returns tables contained
     * @param entity Query entity
     * @param vote Number of duplicated per table for the table to be included in the result set
     * @return Set of tables
     */
    @Override
    public Set<String> search(String entity, int vote)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("No EntityLinker object has been specified");
        }

        else if (this.entityTable == null)
        {
            throw new RuntimeException("No EntityTable object has been specified");
        }

        int entitySignatureIdx = createOrGetSignature(entity);

        if (entitySignatureIdx != -1)
        {
            List<Integer> keys = createKeys(this.permutations.size(), this.bandSize,
                    this.signature.get(entitySignatureIdx).second(), groupSize(), this.hash);
            return super.search(keys, vote);
        }

        return new HashSet<>();
    }

    /**
     * Aggregates all keys into one by merging all sets of types per key into one super-set
     * @param keys Query entities to be aggregated
     * @return Set of tables
     */
    @Override
    public Set<String> agggregatedSearch(String ... keys)
    {
        return agggregatedSearch(1, keys);
    }

    /**
     * Aggregates all keys into one by merging all sets of types per key into one super-set
     * @param vote Number of duplicated per table for the table to be included in the result set
     * @param keys Query entities to be aggregated
     * @return Set of tables
     */
    @Override
    public Set<String> agggregatedSearch(int vote, String ... keys)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("No EntityLinker object has been specified");
        }

        else if (this.entityTable == null)
        {
            throw new RuntimeException("No EntityTable object has been specified");
        }

        Set<String> mergedElements = new HashSet<>();

        for (String key : keys)
        {
            mergedElements.addAll(elements(key));
        }

        Set<Integer> aggregatedBitVector = bitVector(mergedElements);
        int signatureIdx = createOrGetSignature(Id.alloc(), aggregatedBitVector);

        if (signatureIdx != -1)
        {
            List<Integer> bandKeys = createKeys(this.permutations.size(), this.bandSize,
                    this.signature.get(signatureIdx).second(), groupSize(), this.hash);
            return super.search(bandKeys, vote);
        }

        return new HashSet<>();
    }

    private static int concat(int a, int b)
    {
        return (int) (b + a * Math.pow(10, Math.ceil(Math.log10(b + 1))));
    }
}
