package dk.aau.cs.dkwe.edao.calypso.datalake.search;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import dk.aau.cs.dkwe.edao.calypso.datalake.loader.Stats;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Result implements Externalizable
{
    private int k, size;
    private List<Pair<File, Double>> tableScores;
    private Map<String, Stats> stats;

    public Result(int k, List<Pair<File, Double>> tableScores, Map<String, Stats> tableStats)
    {
        this.k = k;
        this.size = Math.min(k, tableScores.size());
        this.tableScores = tableScores;
        this.stats = tableStats;
    }

    public Result(int k, Map<String, Stats> tableStats, Pair<File, Double> ... tableScores)
    {
        this(k, List.of(tableScores), tableStats);
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
            if (e1.getSecond().equals(e2.getSecond()))
                return 0;

            return e1.getSecond() > e2.getSecond() ? -1 : 1;
        });

        if (this.tableScores.size() < this.k + 1)
            return this.tableScores.iterator();

        return this.tableScores.subList(0, this.k).iterator();
    }

    /**
     * Writes results in JSON format
     * @param out the stream to write the results to
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        Iterator<Pair<File, Double>> results = getResults();
        JsonObject jsonObj = new JsonObject();
        JsonArray innerObjs = new JsonArray();

        while (results.hasNext())
        {
            Pair<File, Double> result = results.next();
            String tableID = result.getFirst().getName();
            int entityMappedRows = this.stats.get(tableID).entityMappedRows();
            double entityMappedRowsFraction = this.stats.get(tableID).fractionOfEntityMappedRows();
            List<Double> tupleScores = this.stats.get(tableID).queryRowScores();
            List<List<Double>> tupleVectors = this.stats.get(tableID).queryRowVectors();
            List<List<String>> tupleQueryAlignment = this.stats.get(tableID).tupleQueryAlignment();

            JsonObject tmp = new JsonObject();
            tmp.addProperty("tableID", result.getFirst().getName());
            tmp.addProperty("score", result.getSecond());
            tmp.addProperty("numEntityMappedRows", String.valueOf(entityMappedRows));
            tmp.addProperty("fractionOfEntityMappedRows", String.valueOf(entityMappedRowsFraction));
            tmp.addProperty("tupleScores", String.valueOf(tupleScores));
            tmp.addProperty("tupleVectors", String.valueOf(tupleVectors));

            if (tupleQueryAlignment != null)
            {
                tmp.addProperty("tupleQueryAlignment", String.valueOf(tupleQueryAlignment));
            }

            innerObjs.add(tmp);
        }

        jsonObj.add("scores", innerObjs);
        out.writeObject(jsonObj);
    }

    @Override
    public void readExternal(ObjectInput in)
    {
        throw new UnsupportedOperationException("Reading result is not supports");
    }
}
