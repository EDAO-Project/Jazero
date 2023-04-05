package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.algorithm;

import dk.aau.cs.dkwe.edao.calypso.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.connector.Neo4jEndpoint;

import java.util.*;

public class Ppr
{
    /**
     * Returns a List<List<Double>> with the weight scores
     * @param connector: A Neo4jEndpoint to the graph database
     * @param queryEntities: A 2D list of the query tuples. Indexed by (tupleID, entityPosition) 
     * @param entityToIDF: A mapping of each entity to its IDF score
     * @return a List<List<Double>> with the respective weights for each query entity in the same order as in `queryEntities`
    */
    public static List<List<Double>> getWeights(Neo4jEndpoint connector, List<List<String>> queryEntities,
                                                Map<String, Double> entityToIDF)
    {
        List<List<Double>> weights = new ArrayList<>();
        List<List<Double>> edgeRatioScores = getEdgeRatioScores(connector, queryEntities);
        List<List<Double>> idfRatioScores = getIDFRatioScores(queryEntities, entityToIDF);
        double alpha1 = 0.5;
        double alpha2 = 0.5;

        for (int i = 0; i<queryEntities.size(); i++)
        {
            List<Double> weightPerQueryTuple = new ArrayList<>();

            for (int j = 0; j<queryEntities.get(i).size(); j++)
            {
                Double weight = alpha1 * edgeRatioScores.get(i).get(j) + alpha2 * idfRatioScores.get(i).get(j);
                weightPerQueryTuple.add(weight);
            }

            weights.add(weightPerQueryTuple);
        }

        return weights;
    }

    public static List<List<Double>> getWeights(Neo4jEndpoint connector, Table<String> query, Map<String, Double> entityToIDF)
    {
        return getWeights(connector, queryToMatrix(query), entityToIDF);
    }

    private static List<List<String>> queryToMatrix(Table<String> query)
    {
        List<List<String>> queryEntities = new ArrayList<>();

        for (int i = 0; i < query.rowCount(); i++)
        {
            queryEntities.add(new ArrayList<>(query.getRow(i).size()));

            for (int j = 0; j < query.getRow(i).size(); j++)
            {
                queryEntities.get(i).add(query.getRow(i).get(j));
            }
        }

        return queryEntities;
    }

    /**
     * 
     * @param queryEntities: A 2D list of the query tuples. Indexed by (tupleID, entityPosition) 
     * @return a List<List<Double>> with uniform weights for each entity in the `queryEntities`. All weights are set to 1.0
    */
    public static List<List<Double>> getUniformWeights(List<List<String>> queryEntities)
    {
        List<List<Double>> weights = new ArrayList<>();

        for (int i = 0; i < queryEntities.size(); i++)
        {
            List<Double> weightPerQueryTuple = new ArrayList<>();

            for (int j = 0; j < queryEntities.get(i).size(); j++)
            {
                Double weight = 1.0;
                weightPerQueryTuple.add(weight);
            }

            weights.add(weightPerQueryTuple);
        }

        return weights;
    }

    public static List<List<Double>> getUniformWeights(Table<String> query)
    {
        return getUniformWeights(queryToMatrix(query));
    }

    public static List<List<Double>> getEdgeRatioScores(Neo4jEndpoint connector, List<List<String>> queryEntities)
    {
        // TODO: Computing the number of edges is very slow (Use a constant for now, any better fix?)
        long numNodes = connector.getNumNodes();
        long numEdges = 98900215L;
        Double meanEdgesPerNode = (double) numEdges / numNodes;
        List<List<Double>> edgeRatioScores = new ArrayList<>();

        for (List<String> queryTuple : queryEntities)
        {
            List<Double> edgeScoresPerQueryTuple = new ArrayList<>();

            for (String queryNode : queryTuple)
            {
                // TODO: Currently the number of edges for query nodes is too high compared to the mean,
                // TODO: so maybe scale the rations by a log factor instead?
                double ratio = connector.getNumNeighbors(queryNode) / meanEdgesPerNode;

                if (ratio > 1.0)    // Adjust the ratio by a log factor
                {
                    ratio = 1 + Math.log10(ratio);
                }

                edgeScoresPerQueryTuple.add(ratio);
            }

            edgeRatioScores.add(edgeScoresPerQueryTuple);
        }

        return edgeRatioScores;
    }


    public static List<List<Double>> getIDFRatioScores(List<List<String>> queryEntities, Map<String, Double> entityToIDF)
    {
        // Compute IDF ratio scores
        List<List<Double>> idfRatioScores = new ArrayList<>();
        double idfSum = 0.0;
        int numEntities = 0;

        for (List<String> queryTuple : queryEntities)
        {
            List<Double> idfScoresPerTuple = new ArrayList<>();

            for (String queryNode : queryTuple)
            {
                double score = entityToIDF.get(queryNode);
                idfSum += score;
                numEntities += 1;
                idfScoresPerTuple.add(score);
            }
            idfRatioScores.add(idfScoresPerTuple);
        }

        double mean_IDF = idfSum / numEntities;

        for (int i = 0; i < idfRatioScores.size(); i++)
        {
            for (int j = 0; j < idfRatioScores.get(i).size(); j++)
            {
                idfRatioScores.get(i).set(j, idfRatioScores.get(i).get(j) / mean_IDF);
            }
        }

        return idfRatioScores;
    }


    /**
     * @return An updated `queryEntities` list with only one query tuple that includes all query entities
     * from all query tuples 
     * 
    */
    public static List<List<String>> combineQueryTuplesInSingleTuple(List<List<String>> queryEntities)
    {
        List<List<String>> newQueryEntities = new ArrayList<>();
        Set<String> entitiesSet = new HashSet<>();

        for (List<String> queryTuple :  queryEntities)
        {
            entitiesSet.addAll(queryTuple);
        }

        List<String> newQueryTuple = new ArrayList<>(entitiesSet);
        newQueryEntities.add(newQueryTuple);
        return newQueryEntities;
    }

    public static Table<String> combineQueryTuplesInSingleTuple(Table<String> query)
    {
        List<List<String>> temp = combineQueryTuplesInSingleTuple(queryToMatrix(query));
        return TableParser.toTable(temp);
    }
}
