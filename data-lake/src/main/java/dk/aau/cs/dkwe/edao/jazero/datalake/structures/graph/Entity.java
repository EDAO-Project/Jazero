package dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Embedding;

import java.io.Serializable;
import java.util.List;

public class Entity implements Comparable<Entity>, Serializable
{
    private final String uri;
    private final List<Type> types;
    private final List<String> predicates;
    private final Embedding embedding;
    private double idf = 1;

    public Entity(String uri)
    {
        this(uri, List.of(), List.of(), new Embedding(List.of()));
    }

    public Entity(String uri, List<Type> types, List<String> predicates, Embedding embedding)
    {
        this.uri = uri;
        this.types = types;
        this.predicates = predicates;
        this.embedding = embedding;
    }

    public Entity(String uri, double idf, List<Type> types, List<String> predicates, Embedding embedding)
    {
        this(uri, types, predicates, embedding);
        this.idf = idf;
    }

    public String getUri()
    {
        return this.uri;
    }

    public List<Type> getTypes()
    {
        return this.types;
    }

    public List<String> getPredicates()
    {
        return this.predicates;
    }

    public Embedding getEmbedding()
    {
        return this.embedding;
    }

    public double getIDF()
    {
        return this.idf;
    }

    public void setIDF(double idf)
    {
        this.idf = idf;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Entity other))
            return false;

        return this.uri.equals(other.uri) && this.types.equals(other.types) && this.predicates.equals(other.predicates) && this.embedding.equals(other.embedding);
    }

    @Override
    public int compareTo(Entity o)
    {
        if (equals(o))
            return 0;

        return this.uri.compareTo(o.getUri());
    }
}
