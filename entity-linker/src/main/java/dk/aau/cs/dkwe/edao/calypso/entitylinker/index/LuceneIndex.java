package dk.aau.cs.dkwe.edao.calypso.entitylinker.index;

import dk.aau.cs.dkwe.edao.calypso.datalake.store.Index;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.io.Serializable;

public class LuceneIndex implements Index<String, String>, Serializable
{
    private IndexSearcher searcher;
    private Analyzer analyzer;
    public static final String URI_FIELD = "uri";
    public static final String TEXT_FIELD = "text";

    public LuceneIndex(IndexSearcher searcher, Analyzer analyzer)
    {
        this.searcher = searcher;
        this.analyzer = analyzer;
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
            Query query = new FuzzyQuery(new Term(URI_FIELD, key));
            ScoreDoc[] hits = this.searcher.search(query, 10).scoreDocs;

            if (hits.length == 0)
            {
                return null;
            }

            Document doc = this.searcher.doc(hits[0].doc);
            return doc.get(URI_FIELD);
        }

        catch (IOException e)
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
