package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.Stats;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.ParsingException;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Result
{
    private final int k, size;
    private final List<Pair<File, Double>> tableScores;
    private final Map<String, Stats> stats;
    private final double runtime;
    private double reduction = 0;
    private static final double MAX_MEM_LIMIT_FACTOR = 0.45;    // In case the query is run locally, the total memory consumption will be double to store the results

    public Result(int k, List<Pair<File, Double>> tableScores, double runtime, Map<String, Stats> tableStats)
    {
        this.k = k;
        this.size = Math.min(k, tableScores.size());
        this.tableScores = tableScores;
        this.stats = tableStats;
        this.runtime = runtime;
    }

    public Result(int k, Map<String, Stats> tableStats, double runtime, Pair<File, Double> ... tableScores)
    {
        this(k, List.of(tableScores), runtime, tableStats);
    }

    public void setReduction(double reduction)
    {
        this.reduction = reduction;
    }

    public int getK()
    {
        return this.k;
    }

    public int getSize()
    {
        return this.size;
    }

    public Iterator<Pair<File, Double>> getResults()
    {
        this.tableScores.sort((e1, e2) -> {
            if (e1.second().equals(e2.second()))
                return 0;

            return e1.second() > e2.second() ? -1 : 1;
        });

        if (this.tableScores.size() < this.k + 1)
            return this.tableScores.iterator();

        return this.tableScores.subList(0, this.k).iterator();
    }

    @Override
    public String toString()
    {
        JsonObject object = new JsonObject();
        JsonArray array = new JsonArray(getK());
        Iterator<Pair<File, Double>> scores = getResults();
        double availableMemory = Runtime.getRuntime().freeMemory() * MAX_MEM_LIMIT_FACTOR;
        AtomicLong usedMemory = new AtomicLong(0);

        while (scores.hasNext())
        {
            if (usedMemory.get() > availableMemory)
            {
                object.addProperty("message", "Result set was limited due to not enough memory");
                break;
            }

            Pair<File, Double> score = scores.next();
            JsonObject jsonScore = new JsonObject();
            String tableId = score.first().getName();
            jsonScore.add("table ID", new JsonPrimitive(tableId));

            try
            {
                Table<String> table = TableParser.parse(score.first());
                int rowCount = table.rowCount();
                JsonArray rows = new JsonArray(rowCount);

                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
                {
                    Table.Row<String> row = table.getRow(rowIdx);
                    JsonArray column = new JsonArray(row.size());
                    row.forEach(cell -> {
                        JsonObject cellObject = new JsonObject();
                        cellObject.addProperty("text", cell);
                        column.add(cellObject);
                        usedMemory.setPlain(usedMemory.get() + 5 + cell.length());
                    });

                    rows.add(column);
                    usedMemory.setPlain(usedMemory.get() + 10);
                }

                jsonScore.add("table", rows);
                jsonScore.add("score", new JsonPrimitive(score.second()));
                array.add(jsonScore);
                usedMemory.setPlain(usedMemory.get() + 25 + tableId.length());
            }

            catch (ParsingException e) {}
        }

        object.add("scores", array);
        object.addProperty("runtime", this.runtime);
        object.addProperty("reduction", this.reduction);

        return object.getAsString();
    }
}
