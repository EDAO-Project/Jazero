package dk.aau.cs.dkwe.edao.calypso.entitylinker.index;

import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Logger;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdf.model.impl.PropertyImpl;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class LuceneFactory
{
    public static boolean isBuild()
    {
        File dir = new File(Configuration.getLuceneDir());
        return dir.exists();
    }

    public static void build(File kgDir, boolean verbose) throws IOException
    {
        Analyzer analyzer = new StandardAnalyzer();
        Path indexPath = Files.createDirectory(new File(Configuration.getLuceneDir()).toPath());
        Directory directory = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        int prog = 0, filesCount = kgDir.listFiles().length;
        Set<Node> entities = new HashSet<>();

        if (verbose)
        {
            Logger.logNewLine(Logger.Level.INFO, "Building Lucene index...");
        }

        for (File kgFile : Objects.requireNonNull(kgDir.listFiles(f -> f.getName().endsWith(".ttl"))))
        {
            if (verbose)
            {
                Logger.log(Logger.Level.INFO, ((prog++ / filesCount) * 100) + " KG files indexed into Lucene index ("
                        + kgFile.getName() + ")");
            }

            try
            {
                Model m = ModelFactory.createDefaultModel();
                m.read(new FileInputStream(kgFile), null, "TTL");
                ExtendedIterator<Triple> iter = m.getGraph().find();

                while (iter.hasNext())
                {
                    Triple triple = iter.next();
                    entities.add(triple.getSubject());

                    if (triple.getPredicate().hasURI("http://www.w3.org/2000/01/rdf-schema#label"))
                    {
                        Document doc = new Document();
                        doc.add(new Field(LuceneIndex.URI_FIELD, triple.getSubject().getURI(), TextField.TYPE_STORED));
                        doc.add(new Field(LuceneIndex.TEXT_FIELD, triple.getSubject().getURI() + " " +
                                triple.getPredicate().getURI(), TextField.TYPE_STORED));
                        writer.addDocument(doc);
                        entities.remove(triple.getSubject());
                    }
                }

                for (Node entity : entities)
                {
                    Document doc = new Document();
                    doc.add(new Field(LuceneIndex.URI_FIELD, entity.getURI(), TextField.TYPE_STORED));
                    doc.add(new Field(LuceneIndex.TEXT_FIELD, entity.getURI(), TextField.TYPE_STORED));
                    writer.addDocument(doc);
                }
            }

            catch (FileNotFoundException e)
            {
                if (verbose)
                {
                    Logger.logNewLine(Logger.Level.ERROR, "KG file '" + kgFile.getAbsolutePath() + "' was not found");
                }
            }

            finally
            {
                writer.close();
            }
        }
    }

    public static LuceneIndex get() throws IOException
    {
        Directory directory = FSDirectory.open(new File(Configuration.getLuceneDir()).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);

        return new LuceneIndex(new IndexSearcher(reader), new StandardAnalyzer());
    }
}
