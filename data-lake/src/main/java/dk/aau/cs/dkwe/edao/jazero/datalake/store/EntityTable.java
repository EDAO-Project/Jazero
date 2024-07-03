package dk.aau.cs.dkwe.edao.jazero.datalake.store;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Type;

import java.io.Serializable;
import java.util.*;

/**
 * Hash index of KG entities
 */
public class EntityTable implements Index<Id, Entity>, Serializable
{
    private final Map<Id, Entity> idx = new HashMap<>();

    @Override
    public void insert(Id key, Entity value)
    {
        this.idx.put(key, value);
    }

    @Override
    public boolean remove(Id key)
    {
        return this.idx.remove(key) != null;
    }

    @Override
    public Entity find(Id key)
    {
        return this.idx.get(key);
    }

    @Override
    public boolean contains(Id key)
    {
        return this.idx.containsKey(key);
    }

    @Override
    public int size()
    {
        return this.idx.size();
    }

    @Override
    public void clear()
    {
        this.idx.clear();
    }

    public Iterator<Type> allTypes()
    {
        Set<Type> types = new HashSet<>();

        for (Entity entity : this.idx.values())
        {
            types.addAll(entity.getTypes());
        }

        return types.iterator();
    }

    public Iterator<String> allPredicates()
    {
        Set<String> predicates = new HashSet<>();

        for (Entity entity : this.idx.values())
        {
            predicates.addAll(entity.getPredicates());
        }

        return predicates.iterator();
    }

    public Iterator<Id> allIds()
    {
        return this.idx.keySet().iterator();
    }
}
