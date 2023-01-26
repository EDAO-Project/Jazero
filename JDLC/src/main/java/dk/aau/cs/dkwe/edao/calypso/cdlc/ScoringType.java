package dk.aau.cs.dkwe.edao.calypso.cdlc;

import dk.aau.cs.dkwe.edao.calypso.datalake.search.TableSearch;

public enum ScoringType
{
    TYPE(null),
    EMBEDDINGS_NORM(TableSearch.CosineSimilarityFunction.NORM_COS),
    EMBEDDINGS_ABS(TableSearch.CosineSimilarityFunction.ABS_COS),
    EMBEDDINGS_ANG(TableSearch.CosineSimilarityFunction.ANG_COS);

    private TableSearch.CosineSimilarityFunction cosineFunction;

    ScoringType(TableSearch.CosineSimilarityFunction cosineSimFunction)
    {
        this.cosineFunction = cosineSimFunction;
    }

    public boolean useCosineSimilarity()
    {
        return this.cosineFunction != null;
    }

    public TableSearch.CosineSimilarityFunction cosineSimilarityFunction()
    {
        return this.cosineFunction;
    }
}
