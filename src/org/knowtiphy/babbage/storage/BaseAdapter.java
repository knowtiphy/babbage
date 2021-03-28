package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.LoggerUtils;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static org.knowtiphy.utils.IProcedure.wrap;

//	base class for account adapters

public abstract class BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseAdapter.class.getName());

	protected String id;
	protected final String type;
	protected final Dataset cache;

	protected final Map<String, BiFunction<String, Model, Future<?>>> operations = new HashMap<>();

	private final ListenerManager listenerManager;
	private final BlockingDeque<Runnable> notificationQ;

	public BaseAdapter(String type, Dataset messageDatabase,
					   ListenerManager newListenerManager, BlockingDeque<Runnable> notificationQ)
	{
		this.type = type;
		this.cache = messageDatabase;
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

	public abstract void sync() throws UnsupportedOperationException;

	@Override
	public Future<?> sync(String fid) throws ExecutionException, InterruptedException
	{
		return new FutureTask<>(() -> 1);
	}

	protected void notifyListeners(Model change)
	{
		notificationQ.add(() -> listenerManager.notifyListeners(change));
	}

	//	apply a change to the database represented by a delta (a collection of adds and deletes)

	protected static Delta apply(Dataset dbase, Delta delta)
	{
		dbase.begin(ReadWrite.WRITE);
		try
		{
			//	do deletes before adds in case adds are replacing things that are being deleted
			dbase.getDefaultModel().remove(delta.getDeletes());
			dbase.getDefaultModel().add(delta.getAdds());
			dbase.commit();
		}
		catch (Exception ex)
		{
			//	if this happens were are in deep shit with no real way of recovering
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			dbase.abort();
		}
		finally
		{
			dbase.end();
		}

		return delta;
	}

	protected Delta apply(Delta delta)
	{
		return apply(cache, delta);
	}

	protected void applyAndNotify(Delta delta, EventSetBuilder builder)
	{
		apply(delta);
		notifyListeners(builder.model);
	}

	//	run a string query inside a read transaction on the data set

	public static ResultSet query(Dataset dataset, String query) throws StorageException
	{
		var infModel = ModelFactory.createRDFSModel(dataset.getDefaultModel());

		dataset.begin(ReadWrite.READ);
		try
		{
			//	TODO -- longer term we need a better solution than this
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

	protected <E> void query(IProcedure query)
	{
		query(wrap(query));
	}


	@Override
	public Future<?> markMessagesAsAnswered(Collection<String> messageIds, String folderId, boolean flag)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> markMessagesAsJunk(Collection<String> messageIds, String folderId, boolean flag)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
										boolean delete)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								  boolean delete) throws MessagingException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> appendMessages(String folderId, Message[] messages)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Message createMessage(MessageModel model)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> send(Model model)
	{
		throw new UnsupportedOperationException();
	}
}