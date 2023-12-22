package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.EmbeddingsIndex;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

public abstract class AbstractSearch implements Search
{
    private final EntityLinking linker;
    private final EntityTable entityTable;
    private final EntityTableLink entityTableLink;
    private final EmbeddingsIndex<Id> embeddingIdx;

    protected AbstractSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                             EmbeddingsIndex<Id> embeddingIndex)
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

    public EmbeddingsIndex<Id> getEmbeddingIndex()
    {
        return this.embeddingIdx;
    }

    protected abstract Result abstractSearch(Table<String> query);
    protected abstract long abstractElapsedNanoSeconds();
}
