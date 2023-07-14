package dk.aau.cs.dkwe.edao.calypso.entitylinker.index;

import dk.aau.cs.dkwe.edao.calypso.datalake.store.Index;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.beans.XMLEncoder;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class LuceneIndex implements Index<String, String>, Serializable
{
    private final IndexSearcher searcher;
    private final QueryParser parser = new QueryParser(TEXT_FIELD, new StandardAnalyzer());
    public static final String URI_FIELD = "uri";
    public static final String TEXT_FIELD = "text";
    private static final int CACHE_MAX = 5000;

    private final Map<String, String> cache =
            Collections.synchronizedMap(new LinkedHashMap<String, String>(CACHE_MAX, 0.75f, true) {
                @Override
                public boolean removeEldestEntry(Map.Entry eldest)
                {
                    return size() > CACHE_MAX;
                }
            });

    public LuceneIndex(IndexSearcher searcher)
    {
        this.searcher = searcher;
    }

    @Override
    public void insert(String key, String value)
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }

    @Override
    public boolean remove(String key)
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }

    @Override
    public String find(String key)
    {
        String link = this.cache.get(key);

        if (link != null)
        {
            return link;
        }

        try
        {
            Query query = this.parser.parse(key);
            ScoreDoc[] hits = this.searcher.search(query, 1).scoreDocs;

            if (hits.length == 0)
            {
                return null;
            }

            Document doc = this.searcher.doc(hits[0].doc);
            link = doc.get(URI_FIELD);
            this.cache.put(key, link);
            return doc.get(URI_FIELD);
        }

        catch (IOException | ParseException | IllegalArgumentException e)
        {
            return null;
        }
    }

    @Override
    public boolean contains(String key)
    {
        return false;
    }

    @Override
    public int size()
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }
}
