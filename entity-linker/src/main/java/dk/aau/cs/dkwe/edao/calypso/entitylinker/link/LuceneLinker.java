package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

import dk.aau.cs.dkwe.edao.calypso.entitylinker.index.LuceneIndex;

public class LuceneLinker implements EntityLink<String, String>
{
    private LuceneIndex luceneIndex;

    public LuceneLinker(LuceneIndex luceneIndex)
    {
        this.luceneIndex = luceneIndex;
    }

    @Override
    public String link(String key)
    {
        return this.luceneIndex.find(key);
    }
}
