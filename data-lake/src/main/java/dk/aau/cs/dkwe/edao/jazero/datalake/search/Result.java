package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import com.google.gson.*;
import dk.aau.cs.dkwe.edao.jazero.datalake.loader.Stats;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.ParsingException;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.SimpleTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Result
{
    private int k, size;
    private List<Pair<File, Double>> tableScores;
    private List<Pair<Double, Table<String>>> resultTables;
    private boolean limitedByMemory = false;
    private final Map<String, Stats> stats;
    private double runtime;
    private double reduction = 0;
    private static final double MAX_MEM_LIMIT_FACTOR = 0.45;    // In case the query is run locally, the total memory consumption will be double to store the results

    public Result(int k, List<Pair<File, Double>> tableScores, double runtime, double reduction, Map<String, Stats> tableStats)
    {
        this.k = k;
        this.size = Math.min(k, tableScores.size());
        this.tableScores = tableScores;
        this.stats = tableStats;
        this.runtime = runtime;
        this.reduction = reduction;
        this.resultTables = null;
    }

    private Result(String json)
    {
        this.resultTables = parseTables(json);
        this.stats = Map.of();
    }

    public int getK()
    {
        return this.k;
    }

    public int getSize()
    {
        return this.size;
    }

    public double getRuntime()
    {
        return this.runtime;
    }

    public double getReduction()
    {
        return this.reduction;
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
        List<Pair<Double, Table<String>>> tables = getTables();
        JsonObject object = new JsonObject();
        JsonArray array = new JsonArray(getK());

        for (Pair<Double, Table<String>> score : tables)
        {
            Table<String> table = score.second();
            JsonObject jsonScore = new JsonObject();
            JsonArray jsonHeaders = new JsonArray();

            for (String header : table.getColumnLabels())
            {
                jsonHeaders.add(header);
            }

            jsonScore.add("table ID", new JsonPrimitive(table.getId()));
            jsonScore.add("headers", jsonHeaders);

            try
            {
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
                    });

                    rows.add(column);
                }

                jsonScore.add("table", rows);
                jsonScore.add("score", new JsonPrimitive(score.first()));
                array.add(jsonScore);
            }

            catch (ParsingException e) {}
        }

        object.add("scores", array);
        object.addProperty("runtime", this.runtime);
        object.addProperty("reduction", this.reduction);

        if (this.limitedByMemory)
        {
            object.addProperty("message", "Result set was limited due to not enough memory");
        }

        return object.toString();
    }

    private List<Pair<Double, Table<String>>> parseTables()
    {
        Iterator<Pair<File, Double>> scores = getResults();
        double availableMemory = Runtime.getRuntime().freeMemory() * MAX_MEM_LIMIT_FACTOR;
        AtomicLong usedMemory = new AtomicLong(0);
        List<Pair<Double, Table<String>>> tables = new ArrayList<>();

        while (scores.hasNext())
        {
            if (usedMemory.get() > availableMemory)
            {
                this.limitedByMemory = true;
                return tables;
            }

            Pair<File, Double> score = scores.next();
            String tableId = score.first().getName();
            Table<String> parsedTable = TableParser.parse(score.first());
            Table<String> table = new SimpleTable<>(tableId, parsedTable.toList(), parsedTable.getColumnLabels());
            int rowCount = table.rowCount();
            tables.add(new Pair<>(score.second(), table));

            for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
            {
                Table.Row<String> row = table.getRow(rowIdx);
                row.forEach(cell -> usedMemory.setPlain(usedMemory.get() + 5 + cell.length()));
                usedMemory.setPlain(usedMemory.get() + 25 + tableId.length());
            }
        }

        return tables;
    }

    private List<Pair<Double, Table<String>>> parseTables(String json)
    {
        JsonElement jsonResult = JsonParser.parseString(json);
        double runtime = jsonResult.getAsJsonObject().get("runtime").getAsDouble(),
                reduction = jsonResult.getAsJsonObject().get("reduction").getAsDouble();
        JsonArray jsonResultArray = jsonResult.getAsJsonObject().getAsJsonArray("scores");
        this.runtime = runtime;
        this.reduction = reduction;
        this.k = jsonResultArray.size();

        List<Pair<File, Double>> results = new ArrayList<>(this.k);
        List<Pair<Double, Table<String>>> tables = new ArrayList<>(this.k);
        double availableMemory = Runtime.getRuntime().freeMemory() * MAX_MEM_LIMIT_FACTOR;
        AtomicLong usedMemory = new AtomicLong(0);

        for (JsonElement element : jsonResultArray)
        {
            String tableId = element.getAsJsonObject().get("table ID").getAsString();
            File tableFile = new File(tableId);
            double score = element.getAsJsonObject().get("score").getAsDouble();
            results.add(new Pair<>(tableFile, score));

            if (usedMemory.get() < availableMemory)
            {
                List<List<String>> tableAsList = new ArrayList<>();
                JsonArray jsonTableRows = element.getAsJsonObject().getAsJsonArray("table");

                for (JsonElement row : jsonTableRows)
                {
                    List<String> rowAsList = new ArrayList<>();
                    JsonArray cells = row.getAsJsonArray();

                    for (JsonElement cell : cells)
                    {
                        String text = cell.getAsJsonObject().get("text").getAsString();
                        rowAsList.add(text);
                        usedMemory.setPlain(usedMemory.get() + 25 + text.length());
                    }

                    tableAsList.add(rowAsList);
                    usedMemory.setPlain(usedMemory.get() + 25 + tableId.length());
                }

                JsonArray jsonHeaders = element.getAsJsonObject().getAsJsonArray("headers");
                String[] headers = new String[jsonHeaders.size()];
                int i = 0;

                for (JsonElement header : jsonHeaders)
                {
                    headers[i++] = header.getAsString();
                }

                Table<String> table = new SimpleTable<>(tableId, tableAsList, headers);
                tables.add(new Pair<>(score, table));
            }

            else
            {
                this.limitedByMemory = true;
            }
        }

        this.tableScores = results;
        this.size = tables.size();
        return tables;
    }

    public List<Pair<Double, Table<String>>> getTables()
    {
        if (this.resultTables == null)
        {
            this.resultTables = parseTables();
        }

        return this.resultTables;
    }

    public static Result fromJson(String json)
    {
        return new Result(json);
    }
}
