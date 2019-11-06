package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;

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
	public void close() throws UnsupportedOperationException
	{

	}

	public void addListener(Model accountTriples) throws UnsupportedOperationException
	{
	}

	public String encode(Folder folder) throws MessagingException, UnsupportedOperationException
	{
		return null;
	}

	public Future<?> markMessagesAsAnswered(Collection<String> messageIds, String folderId, boolean flag)
			throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> markMessagesAsRead(Collection<String> messageIds, String folderId, boolean flag)
			throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> markMessagesAsJunk(Collection<String> messageIds, String folderId, boolean flag)
			throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
			boolean delete) throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
			boolean delete) throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> deleteMessages(String folderId, Collection<String> messageIds) throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> appendMessages(String folderId, Message[] messages) throws UnsupportedOperationException
	{
		return null;
	}

	public Message createMessage(MessageModel model)
			throws MessagingException, IOException, UnsupportedOperationException
	{
		return null;
	}

	public FutureTask<?> getSynchTask() throws UnsupportedOperationException
	{
		return null;
	}

	public Future<?> ensureMessageContentLoaded(String messageId, String folderId) throws UnsupportedOperationException
	{
		return null;
	}
}
