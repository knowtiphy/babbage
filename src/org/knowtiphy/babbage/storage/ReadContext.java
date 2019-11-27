package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;

/**
 *
 * @author graham
 */
public class ReadContext implements IReadContext
{
    private final Dataset database;

    public ReadContext(Dataset database)
    {
        this.database = database;
    }

    @Override
    public Model getModel()
    {
        return database.getDefaultModel();
    }

    @Override
    public void start()
    {
        database.begin(ReadWrite.READ);
    }

    public void abort()
    {
        database.abort();
    }

    @Override
    public void end()
    {
        database.end();
    }

    public <E extends Exception> void fail(E ex) throws E
    {
        database.abort();
        throw ex;
    }
}