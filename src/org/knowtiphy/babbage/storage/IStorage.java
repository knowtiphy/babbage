package org.knowtiphy.babbage.storage;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;

import javax.mail.MessagingException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author graham
 */
public interface IStorage extends AutoCloseable
{
	Model getAccounts();

	Model sync(String id) throws ExecutionException, InterruptedException, NoSuchAccountException;

	Future<?> sync(String id, String fid) throws ExecutionException, InterruptedException, NoSuchAccountException;

	Future<?> doOperation(Model operation) throws NoSuchAccountException, NoOperationSpecifiedException;

	ResultSet query(String id, String query) throws NoSuchAccountException;

	void addListener(IStorageListener listener) throws InterruptedException, StorageException, MessagingException;

	void close();

	//Model getSpecialFolders();

	// For time being, stick all extra methods in here
	Map<String, Future<?>> addOldListener(IOldStorageListener listener) throws InterruptedException, StorageException, MessagingException, ExecutionException;

	IReadContext getReadContext();


	Future<?> ensureMessageContentLoaded(String accountId, String folderId, String messageId) throws NoSuchAccountException;

	Future<?> loadAhead(String accountId, String folderId, Collection<String> messageIds) throws NoSuchAccountException;

	void send(MessageModel model) throws StorageException;


	Future<?> moveMessagesToJunk(String accountId, String sourceFolderId, Collection<String> messageIds,
								 String targetFolderId, boolean delete) throws NoSuchAccountException;

	Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
						   String targetFolderId, boolean delete) throws MessagingException, NoSuchAccountException;

	Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds, boolean flag) throws NoSuchAccountException;

	Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds, boolean flag) throws NoSuchAccountException;

	Future<?> send(Model model) throws StorageException;
}