package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.IProcedure;
import org.knowtiphy.utils.LoggerUtils;
import org.knowtiphy.utils.ThrowingSupplier;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.logging.Logger;

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

	//	this needs to go -- use Deltas
	protected void notifyListeners(Model model)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(model));
	}

	//	this needs to go -- use Deltas everywhere
	protected void update(Model adds, Model deletes)
	{
		assert adds != null;
		assert deletes != null;
		update(new Delta(adds, deletes));
	}

	//	update based on a delta
	protected void update(Delta delta)
	{
		messageDatabase.begin(ReadWrite.WRITE);
		messageDatabase.getDefaultModel().remove(delta.getToDelete());
		messageDatabase.getDefaultModel().add(delta.getToAdd());
		messageDatabase.commit();
		messageDatabase.end();
		notifyListeners(delta);
	}

	//	update based on the delta produced by a supplier
	public <E extends Exception> void update(ThrowingSupplier<Delta, E> query) throws E
	{
		update(runQuery(query));
	}

	//	run a query returning T on the database inside a read transaction
	public <T, E extends Exception> T runQuery(ThrowingSupplier<T, E> query) throws E
	{
		messageDatabase.begin(ReadWrite.READ);
		T result = query.get();
		messageDatabase.end();
		return result;
	}

	//	run a query on the database inside a read transaction
	public <E extends Exception> void runQuery(IProcedure<E> query) throws E
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
		}
		messageDatabase.end();
	}
}