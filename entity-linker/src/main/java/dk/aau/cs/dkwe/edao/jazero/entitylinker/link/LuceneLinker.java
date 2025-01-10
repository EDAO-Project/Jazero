package dk.aau.cs.dkwe.edao.jazero.entitylinker.link;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;

import java.util.List;

public class LuceneLinker implements EntityLink<String, String>
{
    private Cache<String, String> cache;
    private KGService kg;

    public LuceneLinker()
    {
        this.cache = CacheBuilder.newBuilder().maximumSize(1000).build();
        this.kg = new KGService(Configuration.getEKGManagerHost(), Configuration.getEKGManagerPort());
    }

    @Override
    public String link(String key)
    {
        String link;

        if ((link = this.cache.getIfPresent(key)) != null)
        {
            return link;
        }

        List<String> kgEntities = this.kg.searchEntities(key);

        if (!kgEntities.isEmpty())
        {
            link = kgEntities.get(0);
            this.cache.put(key, link);

            return link;
        }

        return null;
    }
}
