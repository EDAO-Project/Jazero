package dk.aau.cs.dkwe.edao.jazero.entitylinker.indexing;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.Index;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.io.Serializable;

public class LuceneIndex implements Index<String, String>, Serializable
{
    private final IndexSearcher searcher;
    private final QueryParser parser = new QueryParser(TEXT_FIELD, new StandardAnalyzer());
    public static final String URI_FIELD = "uri";
    public static final String TEXT_FIELD = "text";

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
        try
        {
            Query query = this.parser.parse(key);
            ScoreDoc[] hits = this.searcher.search(query, 1).scoreDocs;

            if (hits.length == 0)
            {
                return null;
            }

            Document doc = this.searcher.doc(hits[0].doc);
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
    public long size()
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }
}
