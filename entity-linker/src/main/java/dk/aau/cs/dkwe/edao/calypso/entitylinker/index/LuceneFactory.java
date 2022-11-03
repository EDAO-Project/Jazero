package dk.aau.cs.dkwe.edao.calypso.entitylinker.index;

import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class LuceneFactory
{
    public static boolean isBuild()
    {
        File dir = new File(Configuration.getLuceneDir());
        return dir.exists();
    }

    public static void build(Map<String, Set<String>> entityDocuments, boolean verbose) throws IOException
    {
        Analyzer analyzer = new StandardAnalyzer();
        Path indexPath = Files.createTempDirectory(Configuration.getLuceneDir());
        Directory directory = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        int prog = 0;

        if (verbose)
        {
            Logger.logNewLine(Logger.Level.INFO, "Building Lucene index...");
        }

        for (Map.Entry<String, Set<String>> entry : entityDocuments.entrySet())
        {
            if (verbose)
            {
                Logger.log(Logger.Level.INFO,  ((prog++ / entityDocuments.size()) * 100) + "% progress");
            }

            Document doc = new Document();
            StringBuilder builder = new StringBuilder();
            builder.append(entry.getKey()).append(" ");

            for (String txt : entry.getValue())
            {
                if (txt != null)
                {
                    builder.append(txt).append(" ");
                }
            }

            builder.deleteCharAt(builder.length() - 1);
            doc.add(new Field(LuceneIndex.TEXT_FIELD, builder.toString(), TextField.TYPE_STORED));
            doc.add(new Field(LuceneIndex.URI_FIELD, entry.getKey(), TextField.TYPE_STORED));
            writer.addDocument(doc);
        }

        writer.close();
    }

    public static LuceneIndex get() throws IOException
    {
        Path indexPath = Files.createTempDirectory(Configuration.getLuceneDir());
        Directory directory = FSDirectory.open(indexPath);
        DirectoryReader reader = DirectoryReader.open(directory);

        return new LuceneIndex(new IndexSearcher(reader), new StandardAnalyzer());
    }
}
