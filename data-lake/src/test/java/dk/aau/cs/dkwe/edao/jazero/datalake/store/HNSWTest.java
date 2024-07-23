package dk.aau.cs.dkwe.edao.jazero.datalake.store;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.hnsw.HNSW;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Embedding;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HNSWTest
{
    private static final File EMBEDDINGS_FILE = new File("src/test/resources/embeddings.txt");
    private HNSW hnsw;
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink tableLinks;

    @BeforeEach
    public void setup()
    {
        List<String> tables = List.of("file1", "file2", "file3");
        this.linker = new EntityLinking("", "");
        this.entityTable = new EntityTable();
        this.tableLinks = new EntityTableLink();

        try (BufferedReader reader = new BufferedReader(new FileReader(EMBEDDINGS_FILE)))
        {
            String line;

            while ((line = reader.readLine()) != null)
            {
                String[] tokens = line.split(" ");
                String uri = tokens[0];
                List<Double> embedding = new ArrayList<>(200);

                for (int i = 1; i < tokens.length; i++)
                {
                    embedding.add(Double.parseDouble(tokens[i]));
                }

                Entity entity = new Entity(uri, List.of(), List.of(), new Embedding(embedding));
                this.linker.addMapping(uri.replace("dbpedia", "wikipedia"), uri);

                Id id = this.linker.uriLookup(uri);
                this.entityTable.insert(id, entity);
                this.tableLinks.insert(id, tables);
            }

            Iterator<Id> idIterator = this.linker.uriIds();
            this.hnsw = new HNSW(Entity::getEmbedding, 200, 2000, 10,
                    this.linker, this.entityTable, this.tableLinks, "");

            while (idIterator.hasNext())
            {
                String uri = this.linker.uriLookup(idIterator.next());
                this.hnsw.insert(uri, new HashSet<>(tables));
            }
        }

        catch (IOException e)
        {
            e.printStackTrace();
            assertEquals(1, 2);
        }
    }

    /*@Test
    public void testSize()
    {
        assertEquals(1592, this.hnsw.size());
    }*/
}
