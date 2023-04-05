package dk.aau.cs.dkwe.edao.calypso.datalake.search;

import dk.aau.cs.dkwe.edao.calypso.datalake.store.EmbeddingsIndex;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;

public abstract class AbstractSearch implements Search
{
    private final EntityLinking linker;
    private final EntityTable entityTable;
    private final EntityTableLink entityTableLink;
    private final EmbeddingsIndex<String> embeddingIdx;

    protected AbstractSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                             EmbeddingsIndex<String> embeddingIndex)
    {
        this.linker = linker;
        this.entityTable = entityTable;
        this.entityTableLink = entityTableLink;
        this.embeddingIdx = embeddingIndex;
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

    public EmbeddingsIndex<String> getEmbeddingIndex()
    {
        return this.embeddingIdx;
    }

    protected abstract Result abstractSearch(Table<String> query);
    protected abstract long abstractElapsedNanoSeconds();
}
