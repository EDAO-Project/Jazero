package dk.aau.cs.dkwe.edao.calypso.datalake.search;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;

public interface Search
{
    Result search(Table<String> query);
    long elapsedNanoSeconds();
}
