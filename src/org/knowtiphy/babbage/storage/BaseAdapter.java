package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.LoggerUtils;
import org.knowtiphy.utils.ThrowingConsumer;
import org.knowtiphy.utils.ThrowingSupplier;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.logging.Logger;

//	base class for account adapters

public abstract class BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseAdapter.class.getName());

	protected String id;
	protected String type;
	protected final Dataset messageDatabase;

	private final OldListenerManager oldListenerManager;
	private final ListenerManager listenerManager;
	private final BlockingDeque<Runnable> notificationQ;

	public BaseAdapter(String type, Dataset messageDatabase,
					   OldListenerManager listenerManager,
					   ListenerManager newListenerManager,
					   BlockingDeque<Runnable> notificationQ)
	{
		this.type = type;
		this.messageDatabase = messageDatabase;
		this.oldListenerManager = listenerManager;
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
	public void initialize(Delta delta) throws Exception
	{
	}

//	@Override
//	public void addInitialTriples(Delta delta)
//	{
//	}

	@Override
	public void close(Model model)
	{
	}

	public Model sync() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> sync(String fid) throws ExecutionException, InterruptedException
	{
		return new FutureTask<>(() -> 1);
	}

	@Override
	public void addListener()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> doOperation(String oid, String type, Model operation)
	{
		throw new UnsupportedOperationException();
	}

	protected void notifyListeners(Model change)
	{
		notificationQ.add(() -> listenerManager.notifyListeners(change));
	}

	//	run a query, which returns a value of type T, inside a read transaction on the database

	protected <T, E extends Exception> T query(ThrowingSupplier<T, E> query) throws E
	{
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			return query.get();
		}
		catch (Exception ex)
		{
			messageDatabase.abort();
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			//noinspection unchecked
			throw (E) ex;
		}
		finally
		{
			messageDatabase.end();
		}
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
		return apply(messageDatabase, delta);
	}

	protected void applyAndNotify(Delta delta, EventSetBuilder builder)
	{
		apply(delta);
		notifyListeners(builder.model);
	}
	//	run a query inside a read transaction on the database

	protected <E extends Exception> void query(IProcedure<E> query) throws E
	{
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			query.call();
		}
		catch (Exception ex)
		{
			messageDatabase.abort();
			ex.printStackTrace(System.out);
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			// noinspection unchecked
			throw (E) ex;
		}
		finally
		{
			messageDatabase.end();
		}
	}

	//	TODO -- all the code below here is crap and needs to go away

	protected void notifyOldListeners(Delta delta)
	{
		notificationQ.add(() -> oldListenerManager.notifyChangeListeners(delta));
	}

	//	run a query, which fills a delta with adds/deletes, inside a read transaction on the database

	protected <E extends Exception> Delta query(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		return query(() ->
		{
			Delta delta = new Delta();
			computeChanges.apply(delta);
			return delta;
		});
	}

	//	run a query, which fills a delta with adds/deletes, inside a read transaction on the database,
	//	and notify any listeners

	protected <E extends Exception> void queryAndNotify(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		notifyOldListeners(query(computeChanges));
	}

	//	apply a change to the database represented by the delta computed from a query running insides a read transaction
	protected <E extends Exception> Delta apply(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		return apply(query(computeChanges));
	}

	//	apply a change to the database represented by the delta computed from a query running
	//	inside a read transaction, and notify listeners
	protected <E extends Exception> void applyAndNotify(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		notifyOldListeners(apply(computeChanges));
	}

	@Override
	public ResultSet query(String queryString)
	{
		var infModel = ModelFactory.createRDFSModel(messageDatabase.getDefaultModel());
		var query = QueryFactory.create(queryString);

		messageDatabase.begin(ReadWrite.READ);
		try
		{
			//	TODO -- longer term we need a better solution than this
			try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel))
			{
				ResultSet results = qexec.execSelect();
				return ResultSetFactory.copyResults(results);//QueryExecutionFactory.create(query, infModel).execSelect());
			}
		}
		catch (Exception ex)
		{
			messageDatabase.abort();
			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
			throw ex;
		}
		finally
		{
			messageDatabase.end();
		}
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
	public Message createMessage(MessageModel model) throws MessagingException, IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> send(Model model)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> ensureMessageContentLoaded(String messageId, String folderId)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> loadAhead(String folderId, Collection<String> messageIds)
	{
		throw new UnsupportedOperationException();
	}

}