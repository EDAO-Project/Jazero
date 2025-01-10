package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.search;

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
import java.util.ArrayList;
import java.util.List;

public class LuceneIndex implements Index<String, List<String>>, Serializable
{
    private final IndexSearcher searcher;
    private final QueryParser parser = new QueryParser(TEXT_FIELD, new StandardAnalyzer());
    public static final String URI_FIELD = "uri";
    public static final String TEXT_FIELD = "text";
    private static final int K = 10;

    public LuceneIndex(IndexSearcher searcher)
    {
        this.searcher = searcher;
    }

    @Override
    public void insert(String key, List<String> value)
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }

    @Override
    public boolean remove(String key)
    {
        throw new UnsupportedOperationException("Not supported: " + this.getClass().getName());
    }

    @Override
    public List<String> find(String key)
    {
        try
        {
            Query query = this.parser.parse(key);
            ScoreDoc[] hits = this.searcher.search(query, K).scoreDocs;
            List<String> ranked = new ArrayList<>();

            for (ScoreDoc hit : hits)
            {
                Document doc = this.searcher.doc(hit.doc);
                String uri = doc.get(URI_FIELD);
                ranked.add(uri);
            }

            return ranked;
        }

        catch (IOException | ParseException | IllegalArgumentException e)
        {
            return new ArrayList<>();
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
