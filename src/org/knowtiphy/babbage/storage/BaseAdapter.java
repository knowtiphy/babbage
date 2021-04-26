package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.LoggerUtils;
import org.knowtiphy.utils.Wrap;

import javax.mail.Message;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Logger;

//	base class for account adapters

public abstract class BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseAdapter.class.getName());

	protected String id;
	protected final String type;
	protected final Dataset cache;

	private final ListenerManager listenerManager;
	private final BlockingDeque<Runnable> notificationQ;

	protected final Map<String, BiFunction<String, Model, Future<?>>> operations = new HashMap<>();

	public BaseAdapter(String type, Dataset cache, ListenerManager newListenerManager,
					   BlockingDeque<Runnable> notificationQ)
	{
		this.type = type;
		this.cache = cache;
		this.listenerManager = newListenerManager;
		this.notificationQ = notificationQ;
	}

	@Override
	public String getId()
	{
		return id;
	}

	@Override
	public String getType()
	{
		return type;
	}

	@Override
	public Future<?> doOperation(String oid, String type, Model operation) throws NoOperationSpecifiedException
	{
		var op = operations.get(type);
		//	TODO -- check we have such an operation
		if (op == null)
		{
			throw new NoOperationSpecifiedException();
		}
		return op.apply(oid, operation);
	}

	protected void notifyListeners(Model change)
	{
		notificationQ.add(() -> listenerManager.notifyListeners(change));
	}

	protected Delta getDelta()
	{
		return new Delta(cache);
	}

	protected void applyAndNotify(Delta delta, EventSetBuilder builder)
	{
		delta.apply();
		notifyListeners(builder.model);
	}

	//	run a string query inside a read transaction on the data set

	public static ResultSet query(Dataset dataset, String query) throws StorageException
	{
		var infModel = ModelFactory.createRDFSModel(dataset.getDefaultModel());

		dataset.begin(ReadWrite.READ);
		try
		{
			//	TODO -- This code runs the query, copies the result into an in memory result set,
			//	and then closes the original query execution context to free server resources
			//	longer term we need a better solution than the copy part
			try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel))
			{
				ResultSet results = qexec.execSelect();
				return ResultSetFactory.copyResults(results);
			}
		}
		catch (Exception ex)
		{
			dataset.abort();
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			throw new StorageException(ex);
		}
		finally
		{
			dataset.end();
		}
	}

	//	run a query which returns a value of type T, inside a read transaction on the database

	protected <T> T query(Supplier<T> query)
	{
		cache.begin(ReadWrite.READ);
		try
		{
			return query.get();
		}
		catch (Exception ex)
		{
			cache.abort();
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			throw ex;
		}
		finally
		{
			cache.end();
		}
	}

	//	run a query inside a read transaction on the database

	protected void query(IProcedure query)
	{
		query(Wrap.wrap(query));
	}

	@Override
	public Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
										boolean delete)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								  boolean delete)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> appendMessages(String folderId, Message[] messages)
	{
		throw new UnsupportedOperationException();
	}
}