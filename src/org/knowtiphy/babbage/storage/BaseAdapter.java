package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
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
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

// Actual adapters will override the methods that they actually use

public abstract class BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseAdapter.class.getName());

	protected String id;
	protected String type;
	protected final Dataset messageDatabase;
	protected final OldListenerManager listenerManager;
	protected final ListenerManager newListenerManager;
	protected final BlockingDeque<Runnable> notificationQ;

	public BaseAdapter(String type, Dataset messageDatabase,
					   OldListenerManager listenerManager,
					   ListenerManager newListenerManager,
					   BlockingDeque<Runnable> notificationQ)
	{
		this.type = type;
		this.messageDatabase = messageDatabase;
		this.listenerManager = listenerManager;
		this.newListenerManager = newListenerManager;
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
	public ResultSet query(String query)
	{
		var infModel = ModelFactory.createRDFSModel(messageDatabase.getDefaultModel());
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			return ResultSetFactory.copyResults(QueryExecutionFactory.create(query, infModel).execSelect());
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
	public void initialize() throws Exception
	{
	}

	@Override
	public Future<?> doOperation(String oid, String type, Model operation)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void sync(String fid) throws ExecutionException, InterruptedException
	{
	}

	@Override
	public void close()
	{
	}

	@Override
	public Model getSpecialFolders()
	{
		return ModelFactory.createDefaultModel();
	}

	public void addListener()
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> markMessagesAsAnswered(Collection<String> messageIds, String folderId, boolean flag)
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> markMessagesAsRead(Collection<String> messageIds, String folderId, boolean flag)
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> markMessagesAsJunk(Collection<String> messageIds, String folderId, boolean flag)
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
										boolean delete)
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								  boolean delete) throws MessagingException
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> deleteMessages(String folderId, Collection<String> messageIds) throws MessagingException
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> appendMessages(String folderId, Message[] messages)
	{
		throw new UnsupportedOperationException();
	}

	public Message createMessage(MessageModel model) throws MessagingException, IOException
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> send(Model model)
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> ensureMessageContentLoaded(String messageId, String folderId)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<?> loadAhead(String folderId, Collection<String> messageIds)
	{
		throw new UnsupportedOperationException();
	}

	protected void notifyListeners(Collection<Delta> deltas)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(deltas));
	}

	protected void notifyListeners(Delta delta)
	{
		notifyListeners(List.of(delta));
	}

	//	run a query, which returns a value of type T, inside a read transaction on the database

	public <T, E extends Exception> T query(ThrowingSupplier<T, E> query) throws E
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

	//	run a query, which fills a delta with adds/deletes, inside a read transaction on the database

	public <E extends Exception> Delta query(ThrowingConsumer<Delta, E> computeChanges) throws E
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

	public <E extends Exception> void queryAndNotify(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		notifyListeners(query(computeChanges));
	}

	//	apply a change to the database represented by a delta (a collection of adds and deletes)

	protected static Collection<Delta> apply(Dataset dbase, Collection<Delta> deltas)
	{
		dbase.begin(ReadWrite.WRITE);
		try
		{
			//	do deletes before adds in case adds are replacing things that are being deleted
			deltas.forEach(delta -> dbase.getDefaultModel().remove(delta.getDeletes()));
			deltas.forEach(delta -> dbase.getDefaultModel().add(delta.getAdds()));
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

		return deltas;
	}

	//	apply a change to the database represented by the delta computed from a query running insides a read transaction

	protected static Collection<Delta> apply(Dataset dbase, Delta delta)
	{

		return apply(dbase, List.of(delta));
	}

	protected Collection<Delta> apply(Collection<Delta> deltas)
	{
		return apply(messageDatabase, deltas);
	}

	//	apply a change to the database represented by the delta computed from a query running insides a read transaction

	protected Collection<Delta> apply(Delta delta)
	{
		return apply(List.of(delta));
	}

	//	TODO -- this code is crap and needs to go away
	public <E extends Exception> Delta apply(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		return apply(List.of(query(computeChanges))).iterator().next();
	}

	//	apply a change to the database represented by a delta and notify all listeners

//	protected void applyAndNotify(Collection<Delta> deltas)
//	{
//		notifyListeners(apply(deltas));
//	}

//	protected void applyAndNotify(Delta delta)
//	{
//		notifyListeners(apply(delta));
//	}

	//	apply a change to the database represented by the delta computed from a query running insides a read transaction,
	//	and notify listeners
	//	TODO -- this code is crap and needs to go away

	public <E extends Exception> void applyAndNotify(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		notifyListeners(apply(computeChanges));
	}

	//	run a query inside a read transaction on the database

	public <E extends Exception> void query(IProcedure<E> query) throws E
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

	public void notify(Model change)
	{
		notificationQ.add(() -> newListenerManager.notifyListeners(change));
	}

	public void applyAndNotify(Delta delta, EventSetBuilder builder)
	{
		apply(delta);
		notify(builder.model);
	}
}


//	update based on the delta produced by a supplier -- the consumer approach is cleaner
//	public <E extends Exception> void applyAndNotify(ThrowingSupplier<Delta, E> changes) throws E
//	{
//		applyAndNotify(query(changes));
//	}

//	apply a change to the database represented by the delta computed from a query running insides a read transaction,
//	and notify listeners
//
//	protected Model apply(Pair<Delta, Model> change)
//	{
//		var delta = change.fst();
//		messageDatabase.begin(ReadWrite.WRITE);
//		try
//		{
//			messageDatabase.getDefaultModel().remove(delta.getDeletes());
//			messageDatabase.getDefaultModel().add(delta.getAdds());
//			messageDatabase.commit();
//		}
//		catch (Exception ex)
//		{
//			//	if this happens were are in deep shit with no real way of recovering
//			//	TODO -- return an error model of some sort
//			LOGGER.severe(LoggerUtils.exceptionMessage(ex));
//			messageDatabase.abort();
//		}
//		finally
//		{
//			messageDatabase.end();
//		}
//
//		return change.snd();
//	}