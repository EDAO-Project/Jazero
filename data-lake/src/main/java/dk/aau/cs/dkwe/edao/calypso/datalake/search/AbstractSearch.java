package dk.aau.cs.dkwe.edao.calypso.datalake.search;

import dk.aau.cs.dkwe.edao.calypso.datalake.search.*;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;

public abstract class AbstractSearch implements Search
{
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;

    protected AbstractSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink)
    {
        this.linker = linker;
        this.entityTable = entityTable;
        this.entityTableLink = entityTableLink;
    }

    @Override
    public Result search(Table<String> query)
    {
        return abstractSearch(query);
    }

    @Override
    public long elapsedNanoSeconds()
    {
        return abstractElapsedNanoSeconds();
    }

    public EntityLinking getLinker()
    {
        return this.linker;
    }

    public EntityTable getEntityTable()
    {
        return this.entityTable;
    }

    public EntityTableLink getEntityTableLink()
    {
        return this.entityTableLink;
    }

    protected abstract Result abstractSearch(Table<String> query);
    protected abstract long abstractElapsedNanoSeconds();
}
