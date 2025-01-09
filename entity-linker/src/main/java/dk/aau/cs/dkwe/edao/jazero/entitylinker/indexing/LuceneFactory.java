package dk.aau.cs.dkwe.edao.jazero.entitylinker.indexing;

import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Logger;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.iterator.ExtendedIterator;
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
import java.util.Objects;

public class LuceneFactory
{
    public static boolean isBuild()
    {
        File dir = new File(Configuration.getLuceneDir());
        return dir.listFiles() != null && dir.listFiles().length > 1;
    }

    public static void build(File kgDir, boolean verbose) throws IOException
    {
        Analyzer analyzer = new StandardAnalyzer();
        Path indexPath = Files.createDirectory(new File(Configuration.getLuceneDir()).toPath());
        Directory directory = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        double prog = 0.0, filesCount = kgDir.listFiles() == null ? 0 : kgDir.listFiles().length;

        if (verbose)
        {
            Logger.log(Logger.Level.INFO, "Building Lucene index...");
        }

        for (File kgFile : Objects.requireNonNull(kgDir.listFiles(f -> f.getName().endsWith(".ttl"))))
        {
            if (verbose)
            {
                Logger.log(Logger.Level.INFO, ((prog++ / filesCount) * 100) +
                        "% of KG files indexed into Lucene index (" + kgFile.getName() + ")");
            }

            try
            {
                loadEntities(kgFile, writer);
            }

            catch (IOException e)
            {
                if (verbose)
                {
                    Logger.log(Logger.Level.ERROR, "KG file '" + kgFile.getAbsolutePath() + "' was not found");
                }
            }
        }

        writer.close();
        directory.close();
        analyzer.close();
    }

    private static void loadEntities(File kgFile, IndexWriter writer) throws IOException
    {
        Model m = ModelFactory.createDefaultModel();
        m.read(Files.newInputStream(kgFile.toPath()), null, "TTL");
        ExtendedIterator<Triple> iter = m.getGraph().find();

        while (iter.hasNext())
        {
            Triple triple = iter.next();
            String uri = triple.getSubject().getURI();
            Document doc = new Document();
            doc.add(new Field(LuceneIndex.URI_FIELD, uri, TextField.TYPE_STORED));
            doc.add(new Field(LuceneIndex.TEXT_FIELD, uriPostfix(uri), TextField.TYPE_STORED));
            writer.addDocument(doc);
        }
    }

    private static String uriPostfix(String uri)
    {
        String[] uriSplit = uri.split("/");
        return uriSplit[uriSplit.length - 1].replace('_', ' ');
    }

    public static LuceneIndex get() throws IOException
    {
        Directory directory = FSDirectory.open(new File(Configuration.getLuceneDir()).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);

        return new LuceneIndex(new IndexSearcher(reader));
    }
}
