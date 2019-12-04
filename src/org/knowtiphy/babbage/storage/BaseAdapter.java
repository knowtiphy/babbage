package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.LoggerUtils;
import org.knowtiphy.utils.ThrowingConsumer;
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

	public Future<?> ensureMessageContentLoaded(String messageId, String folderId, boolean immediate)
	{
		throw new UnsupportedOperationException();
	}

	protected void notifyListeners(Delta delta)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(delta));
	}

	//	run a query, which returns a value of type T, inside a read transaction on the database

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
			//noinspection unchecked
			throw (E) ex;
		} finally
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

	protected Delta apply(Delta delta)
	{
		messageDatabase.begin(ReadWrite.WRITE);
		try
		{
			messageDatabase.getDefaultModel().remove(delta.getDeletes());
			messageDatabase.getDefaultModel().add(delta.getAdds());
			messageDatabase.commit();
		} catch (Exception ex)
		{
			//	if this happens were are in deep shit with no real way of recovering
			LOGGER.severe(() -> LoggerUtils.exceptionMessage(ex));
			messageDatabase.abort();
		} finally
		{
			messageDatabase.end();
		}

		return delta;
	}

	//	apply a change to the database represented by the delta computed from a query running insides a read transaction

	public <E extends Exception> Delta apply(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		return apply(query(computeChanges));
	}

	//	apply a change to the database represented by a delta and notify all listeners

	protected void applyAndNotify(Delta delta)
	{
		notifyListeners(apply(delta));
	}

	//	apply a change to the database represented by the delta computed from a query running insides a read transaction,
	//	and notify listeners

	public <E extends Exception> void applyAndNotify(ThrowingConsumer<Delta, E> computeChanges) throws E
	{
		notifyListeners(apply(computeChanges));
	}

	//	update based on the delta produced by a supplier -- the consumer approach is cleaner
	public <E extends Exception> void applyAndNotify(ThrowingSupplier<Delta, E> changes) throws E
	{
		applyAndNotify(query(changes));
	}

}