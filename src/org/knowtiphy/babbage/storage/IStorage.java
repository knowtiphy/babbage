package org.knowtiphy.babbage.storage;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;

import javax.mail.MessagingException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * @author graham
 */
public interface IStorage extends AutoCloseable
{
	Model getAccounts();

	Model getAccountInfo(String id);

	Future<?> doOperation(Model operation);

	ResultSet query(String id, String query);

	void addListener(IStorageListener listener) throws InterruptedException, StorageException, MessagingException;

	void close();

	Model getSpecialFolders();

	// For time being, stick all extra methods in here
	Map<String, FutureTask<?>> addOldListener(IOldStorageListener listener) throws InterruptedException, StorageException, MessagingException;

	IReadContext getReadContext();

	void sync(String id, String fid) throws ExecutionException, InterruptedException;

	Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId);

	Future<?> loadAhead(String accountId, String folderId, Collection<String> messageIds);

	void send(MessageModel model) throws StorageException;

	//  TODO -- mark vs expunge
	Future<?> deleteMessages(String accountId, String folderId, Collection<String> messageIds) throws MessagingException;

	Future<?> moveMessagesToJunk(String accountId, String sourceFolderId, Collection<String> messageIds,
								 String targetFolderId, boolean delete);

	Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
						   String targetFolderId, boolean delete) throws MessagingException;

	Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds, boolean flag);

	Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds, boolean flag);

	Future<?> send(Model model) throws StorageException;
}