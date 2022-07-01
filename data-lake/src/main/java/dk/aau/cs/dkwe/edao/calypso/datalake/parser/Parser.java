package dk.aau.cs.dkwe.edao.calypso.datalake.parser;

import java.util.Iterator;

public interface Parser<T> extends Iterator<T>
{
    String nextLexeme();
    EmbeddingsParser.EmbeddingToken prev();
}
