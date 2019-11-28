package org.knowtiphy.babbage.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.Delta;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.utils.IProcedure;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

// Actual adapters will override the methods that they actually use

public abstract class BaseAdapter implements IAdapter
{
	protected final Dataset messageDatabase;
	protected final ListenerManager listenerManager;
	protected final BlockingDeque<Runnable> notificationQ;

	public BaseAdapter(Dataset messageDatabase, ListenerManager listenerManager, BlockingDeque<Runnable> notificationQ)
	{
		this.messageDatabase = messageDatabase;
		this.listenerManager = listenerManager;
		this.notificationQ = notificationQ;
	}

	public void close()
	{
		throw new UnsupportedOperationException();
	}

	public void addListener()
	{
		throw new UnsupportedOperationException();
	}

	public String encode(Folder folder) throws MessagingException
	{
		throw new UnsupportedOperationException();
	}

//	public String encode(DavResource res)
//	{
//		throw new UnsupportedOperationException();
//	}

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

	protected void notifyListeners(TransactionRecorder recorder)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(recorder));
	}

	protected void notifyListeners(Model model)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(model));
	}

	protected void notifyListeners(Model adds, Model deletes)
	{
		notificationQ.add(() -> listenerManager.notifyChangeListeners(adds, deletes));
	}

	protected void update(Model adds, Model deletes)
	{
		assert adds != null;
		assert deletes != null;
		update(new Delta(adds, deletes));
	}

	protected void update(Delta delta)
	{
		messageDatabase.begin(ReadWrite.WRITE);
		messageDatabase.getDefaultModel().remove(delta.getToDelete());
		messageDatabase.getDefaultModel().add(delta.getToAdd());
		messageDatabase.commit();
		messageDatabase.end();
		notifyListeners(delta.getToAdd(), delta.getToDelete());
	}

//	public enum DBWriteEvent{
//		ADD, DELETE;
//	}

//	protected void update(Model model, DBWriteEvent type)
//	{
//		messageDatabase.begin(ReadWrite.WRITE);
//		if (type == DBWriteEvent.ADD)
//		{
//			messageDatabase.getDefaultModel().add(model);
//		}
//
//		if (type == DBWriteEvent.DELETE)
//		{
//			messageDatabase.getDefaultModel().remove(model);
//		}
//
//		messageDatabase.commit();
//		messageDatabase.end();
//		notifyListeners(model);
//
//	}

	//	run a query on the database in a read transaction, where the query can't throw an exception, and returns
	//	a T
	public <T> T runQuery(Supplier<T> query)
	{
		messageDatabase.begin(ReadWrite.READ);
		T result = query.get();
		messageDatabase.end();
		return result;
	}

	//	run a query on the database in a read transaction
	public <E extends Exception> void runQuery(IProcedure<E> query) throws E
	{
		messageDatabase.begin(ReadWrite.READ);
		try
		{
			query.call();
		}
		catch (Exception ex)
		{
			messageDatabase.abort();
			//	TODO -- log it
			ex.printStackTrace(System.err);
			throw ex;
		}
		messageDatabase.end();
	}



}