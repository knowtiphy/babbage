package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.JenaUtils;
import org.knowtiphy.utils.LoggerUtils;
import org.knowtiphy.utils.ThrowingSupplier;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.logging.Logger;

import static org.knowtiphy.babbage.storage.CALDAV.DStore.P;
import static org.knowtiphy.babbage.storage.CALDAV.DStore.R;

// Actual adapters will override the methods that they actually use

public abstract class BaseAdapter implements IAdapter
{
	private static final Logger LOGGER = Logger.getLogger(BaseAdapter.class.getName());

	protected final Dataset messageDatabase;
	protected final ListenerManager listenerManager;
	protected final BlockingDeque<Runnable> notificationQ;

	public BaseAdapter(Dataset messageDatabase, ListenerManager listenerManager, BlockingDeque<Runnable> notificationQ)
	{
		this.messageDatabase = messageDatabase;
		this.listenerManager = listenerManager;
		this.notificationQ = notificationQ;
	}

	public abstract void close();

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
								  boolean delete)
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> deleteMessages(String folderId, Collection<String> messageIds)
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

	public FutureTask<?> getSynchTask()
	{
		throw new UnsupportedOperationException();
	}

	public Future<?> ensureMessageContentLoaded(String messageId, String folderId)
	{
		throw new UnsupportedOperationException();
	}

	protected void notifyListeners(Delta delta)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(delta));
	}

	//	update based on a delta
	protected void update(Delta delta)
	{
		messageDatabase.begin(ReadWrite.WRITE);
		messageDatabase.getDefaultModel().remove(delta.getDeletes());
		messageDatabase.getDefaultModel().add(delta.getAdds());
		messageDatabase.commit();
		messageDatabase.end();
		notifyListeners(delta);
	}

	//	update based on the delta produced by a supplier
	public <E extends Exception> void update(ThrowingSupplier<Delta, E> query) throws E
	{
		update(query(query));
	}

	//	run a query returning T inside a read transaction on the database
	public <T, E extends Exception> T query(ThrowingSupplier<T, E> query) throws E
	{
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			return query.get();
		} catch (Exception ex)
		{
			messageDatabase.abort();
			LOGGER.severe(() -> LoggerUtils.exceptionMessage(ex));
			throw ex;
		} finally
		{
			messageDatabase.end();
		}
	}

	//	run a query inside a read transaction on the database
	public <E extends Exception> void query(IProcedure<E> query) throws E
	{
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			query.call();
		} catch (Exception ex)
		{
			messageDatabase.abort();
			LOGGER.severe(() -> LoggerUtils.exceptionMessage(ex));
			throw ex;
		} finally
		{
			messageDatabase.end();
		}
	}

	public String getStoredTag(String query, String resType)
	{
		String tag;
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			tag = JenaUtils.single(resultSet, soln -> soln.get(resType).toString());
		} finally
		{
			messageDatabase.end();
		}

		return tag;
	}

	public Set<String> getStored(String query, String resType)
	{
		Set<String> stored = new HashSet<>(1000);
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			ResultSet resultSet = QueryExecutionFactory.create(query, messageDatabase.getDefaultModel()).execSelect();
			stored.addAll(JenaUtils.set(resultSet, soln -> soln.get(resType).asResource().toString()));
		} finally
		{
			messageDatabase.end();
		}

		return stored;
	}

	public <T> void updateTriple(Model messageDB, Delta delta, String resURI, String hasProp, T updated)
	{
		delta.delete(messageDB.listStatements(R(messageDB, resURI), P(messageDB, hasProp), (RDFNode) null));
		delta.addL(resURI, hasProp, updated);
	}
}