package dk.aau.cs.dkwe.edao.calypso.datalake.search;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;

import java.io.File;
import java.util.Iterator;
import java.util.List;

public class Result
{
    private int k, size;
    private List<Pair<File, Double>> tableScores;

    public Result(int k, List<Pair<File, Double>> tableScores)
    {
        this.k = k;
        this.size = Math.min(k, tableScores.size());
        this.tableScores = tableScores;
    }

    public Result(int k, Pair<File, Double> ... tableScores)
    {
        this(k, List.of(tableScores));
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
}
