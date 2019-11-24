package org.knowtiphy.babbage.storage;

import org.knowtiphy.babbage.storage.IMAP.MessageModel;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

// Actual adapters will override the methods that they actually use

public abstract class BaseAdapter implements IAdapter
{
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
}
