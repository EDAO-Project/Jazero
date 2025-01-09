package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.search;

import java.util.Collection;

public interface KeywordSearch
{
    KeywordResult search(String keywordQuery);
    boolean constructIndexes(Collection<String> entities, boolean verbose);
    boolean addEntity(String entity);
}
