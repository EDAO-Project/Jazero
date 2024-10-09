package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.hnsw.HNSW;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh.LSHIndex;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

import java.io.File;
import java.util.*;

/**
 * Searches corpus using specified LSH index
 * This class is used for pre-filtering the search space
 */
public class Prefilter extends AbstractSearch
{
    private long elapsed = -1;
    private LSHIndex<String, String> lsh;
    private HNSW hnsw;
    private static final int SIZE_THRESHOLD = 8;
    private static final int SPLITS_SIZE = 3;
    private static final int MIN_EXISTS_IN = 2;

    private Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink)
    {
        super(linker, entityTable, entityTableLink);
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, LSHIndex<String, String> lshIndex)
    {
        this(linker, entityTable, entityTableLink);
        this.lsh = lshIndex;
        this.hnsw = null;
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, HNSW hnsw)
    {
        this(linker, entityTable, entityTableLink);
        this.hnsw = hnsw;
        this.lsh = null;
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        List<Pair<File, Double>> candidates = new ArrayList<>();
        List<Table<String>> subQueries = List.of(query);
        Map<String, Integer> tableCounter = new HashMap<>();
        boolean isQuerySplit = false;

        if (query.rowCount() >= SIZE_THRESHOLD)
        {
            subQueries = split(query, SPLITS_SIZE);
            isQuerySplit = true;
        }

        for (Table<String> subQuery : subQueries)
        {
            Set<String> subCandidates = searchFromTable(subQuery);
            subCandidates.forEach(t -> tableCounter.put(t, tableCounter.containsKey(t) ? tableCounter.get(t) + 1 : 1));
        }

        for (Map.Entry<String, Integer> entry : tableCounter.entrySet())
        {
            if (isQuerySplit && entry.getValue() >= MIN_EXISTS_IN)
            {
                candidates.add(new Pair<>(new File(entry.getKey()), -1.0));
            }

            else if (!isQuerySplit)
            {
                candidates.add(new Pair<>(new File(entry.getKey()), -1.0));
            }
        }

        this.elapsed = System.nanoTime() - start;
        return new Result(candidates.size(), candidates, this.elapsed, 0.0, new HashMap<>());
    }

    private Set<String> searchFromTable(Table<String> query)
    {
        Set<String> candidates = new HashSet<>();

        if (query.rowCount() == 0)
        {
            return candidates;
        }

        int rows = query.rowCount(), columns = query.getRow(0).size();

        for (int column = 0; column < columns; column++)
        {
            Set<String> entities = new HashSet<>(rows);

            for (int row = 0; row < rows; row++)
            {
                if (column < query.getRow(row).size())
                {
                    entities.add(query.getRow(row).get(column));
                }
            }

            Set<String> tables = this.hnsw == null ? searchLSH(entities) : searchHNSW(entities);
            candidates.addAll(tables);
        }

        return candidates;
    }

    private static List<Table<String>> split(Table<String> table, int splitSize)
    {
        List<Table<String>> subTables = new ArrayList<>();
        int rows = table.rowCount();

        for (int i = 0; i < rows;)
        {
            Table<String> subTable = new DynamicTable<>();

            for (int j = 0; j < splitSize && i < rows; i++, j++)
            {
                subTable.addRow(table.getRow(i));
            }

            subTables.add(subTable);
        }

        return subTables;
    }

    private Set<String> searchLSH(Set<String> entities)
    {
        String[] entityArr = new String[entities.size()];
        int i = 0;

        for (String entity : entities)
        {
            entityArr[i++] = entity;
        }

        return this.lsh.agggregatedSearch(entityArr);
    }

    private Set<String> searchHNSW(Set<String> entities)
    {
        Set<String> tables = new HashSet<>();

        for (String entity : entities)
        {
            tables.addAll(this.hnsw.find(entity));
        }

        return tables;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}