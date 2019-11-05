package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;

/**
 * @author graham
 */
public class WriteContext
{
	private final Dataset database;
	@SuppressWarnings("InstanceVariableMayNotBeInitialized")
	private TransactionRecorder recorder;

	public WriteContext(Dataset database)
	{
		this.database = database;
	}

	public void startTransaction()
	{
		recorder = new TransactionRecorder();
		startTransaction(recorder);
	}

	public void startTransaction(TransactionRecorder recorder)
	{
		database.begin(ReadWrite.WRITE);
		database.getDefaultModel().register(recorder);
	}

	public void succeed()
	{
		commit();
		endTransaction();
	}

	public <E extends Exception> void fail(E ex) throws E
	{
		abort();
		endTransaction();
		throw ex;
	}

	public Model getModel()
	{
		return database.getDefaultModel();
	}

	public TransactionRecorder getRecorder()
	{
		return recorder;
	}

	private void commit()
	{
		database.commit();
	}

	private void abort()
	{
		database.abort();
		recorder.abort();
	}

	private void endTransaction()
	{
		database.getDefaultModel().unregister(recorder);
		database.end();
	}
}