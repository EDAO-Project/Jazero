package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public final class KeywordResult
{
    private final List<Pair<String, Double>> results;

    public KeywordResult(List<Pair<String, Double>> results)
    {
        this.results = results;
    }

    public KeywordResult()
    {
        this.results = new ArrayList<>();
    }

    public void addResult(Pair<String, Double> result)
    {
        this.results.add(result);
    }

    public List<Pair<String, Double>> getResults()
    {
        this.results.sort(Comparator.comparing(Pair::second));
        Collections.reverse(this.results);
        return this.results;
    }
}
