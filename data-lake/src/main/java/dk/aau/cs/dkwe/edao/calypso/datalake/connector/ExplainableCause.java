package dk.aau.cs.dkwe.edao.calypso.datalake.connector;

public interface ExplainableCause
{
    String getError();
    String getStackTrace();
}
