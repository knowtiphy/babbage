package org.knowtiphy.babbage.storage;

import org.knowtiphy.babbage.storage.IMAP.MessageModel;

import javax.mail.MessagingException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 *
 * @author graham
 */
public interface IStorage
{
	// For time being, stick all extra methods in here
	Map<String, FutureTask<?>> addListener(IStorageListener listener) throws InterruptedException, StorageException, MessagingException;

	Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId, boolean immediate);

	void send(MessageModel model) throws StorageException;

	IReadContext getReadContext();

	void close();

	//  TODO -- mark vs expunge
	Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds);

	Future<?> moveMessagesToJunk(String accountId, String sourceFolderId, Collection<String> messageIds,
			String targetFolderId, boolean delete);

	Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
			String targetFolderId, boolean delete);

	Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds, boolean flag);

	Future<?> markMessagesAsRead(String accountId, String folderId, Collection<String> messageIds, boolean flag);

	Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds, boolean flag);
}