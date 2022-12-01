package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.aau.cs.dkwe.edao.calypso.entitylinker.index.LuceneIndex;

public class LuceneLinker implements EntityLink<String, String>
{
    private LuceneIndex luceneIndex;
    private Cache<String, String> cache;

    public LuceneLinker(LuceneIndex luceneIndex)
    {
        this.luceneIndex = luceneIndex;
        this.cache = CacheBuilder.newBuilder().maximumSize(1000).build();
    }

    @Override
    public String link(String key)
    {
        String link;

        if ((link = this.cache.getIfPresent(key)) != null)
        {
            return link;
        }

        link = this.luceneIndex.find(key);

        if (link != null)
        {
            this.cache.put(key, link);
        }

        return link;
    }
}
