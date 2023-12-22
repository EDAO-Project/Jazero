package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

public interface Search
{
    Result search(Table<String> query);
    long elapsedNanoSeconds();
}
