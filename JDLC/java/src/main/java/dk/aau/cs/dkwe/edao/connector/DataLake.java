package dk.aau.cs.dkwe.edao.connector;

import dk.aau.cs.dkwe.edao.jazero.communication.Response;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.User;
import dk.aau.cs.dkwe.edao.structures.Query;

import java.util.List;

public interface DataLake
{
    Response ping();
    Result search(Query query, int k, TableSearch.EntitySimilarity entitySimilarity, boolean prefilter);
    Result search(Query query, int k, TableSearch.EntitySimilarity entitySimilarity, int queryWait, boolean prefilter);
    Response keywordSearch(String keyword);
    Response clear();
    Response clearEmbeddings();
    Response addUser(User newUser);
    Response removeUser(User removedUser);
    Response removeTable(String tableId);
    Response count(String uri);
    Response stats();
}
