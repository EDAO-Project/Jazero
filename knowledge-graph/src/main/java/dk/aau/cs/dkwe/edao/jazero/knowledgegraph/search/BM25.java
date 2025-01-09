package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BM25 implements KeywordSearch, Closeable
{
    private Directory dir;
    private DirectoryReader dirReader = null;
    private IndexSearcher searcher = null;
    private QueryParser parser = null;
    private Analyzer analyzer;
    private IndexWriterConfig config = null;
    private IndexWriter writer = null;
    private static final String URI_FIELD = "URI";
    private static final String STR_FIELD = "STR";
    private static final int TOP_K = 10;

    public BM25(String indexDir) throws IOException
    {
        this.dir = FSDirectory.open(new File(indexDir).toPath());
        this.analyzer = new StandardAnalyzer();
    }

    @Override
    public KeywordResult search(String keywordQuery)
    {
        try
        {
            if (this.searcher == null)
            {
                this.dirReader = DirectoryReader.open(this.dir);
                this.searcher = new IndexSearcher(this.dirReader);
                this.parser = new QueryParser(STR_FIELD, this.analyzer);
            }

            Query query = this.parser.parse(keywordQuery);
            List<Pair<String, Double>> rankedResults = new ArrayList<>();
            ScoreDoc[] hits = this.searcher.search(query, TOP_K).scoreDocs;

            for (ScoreDoc doc : hits)
            {
                double score = doc.score;
                String entity = this.searcher.doc(doc.doc).get(URI_FIELD);
                rankedResults.add(new Pair<>(entity, score));
            }

            return new KeywordResult(rankedResults);
        }

        catch (ParseException | IOException e)
        {
            return new KeywordResult();
        }
    }

    @Override
    public boolean constructIndexes(Collection<String> entities, boolean verbose)
    {
        try
        {
            int count = 0, entityCount = entities.size();
            this.config = new IndexWriterConfig(this.analyzer);
            this.writer = new IndexWriter(this.dir, this.config);

            for (String entity : entities)
            {
                addEntity(entity);

                if (verbose)
                {
                    double progress = ((double) ++count / entityCount) * 100;
                    System.out.println("Progress: " + progress);
                }
            }

            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    public boolean addEntity(String entity)
    {
        try
        {
            if (this.config == null)
            {
                this.config = new IndexWriterConfig(new StandardAnalyzer());
                this.writer = new IndexWriter(this.dir, config);
            }

            Document doc = new Document();
            doc.add(new Field(URI_FIELD, entity, TextField.TYPE_STORED));
            doc.add(new Field(STR_FIELD, stringify(entity), TextField.TYPE_STORED));
            this.writer.addDocument(doc);

            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    private static String stringify(String entity)
    {
        String[] split = entity.split(" ");
        StringBuilder builder = new StringBuilder();

        for (String token : split)
        {
            builder.append(token).append(" ");
        }

        return builder.toString();
    }

    @Override
    public void close()
    {
        try
        {
            this.analyzer.close();
            this.writer.close();
            this.dir.close();
        }

        catch (IOException ignored) {}
    }
}
