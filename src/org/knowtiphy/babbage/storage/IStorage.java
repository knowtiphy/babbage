package org.knowtiphy.babbage.storage;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;

import javax.mail.MessagingException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author graham
 */
public interface IStorage extends AutoCloseable
{
	Future<?> doOperation(Model operation) throws NoSuchAccountException, NoOperationSpecifiedException;

	ResultSet query(String query) throws StorageException;

	void addListener(IStorageListener listener) throws InterruptedException, StorageException, MessagingException;

	void close();

	// For time being, stick all extra methods in here
	//	should be in doOp
	Model sync(String id) throws ExecutionException, InterruptedException, NoSuchAccountException;

	//	should be in doOp
	Future<?> sync(String id, String fid) throws ExecutionException, InterruptedException, NoSuchAccountException;

	IReadContext getReadContext();

	void send(MessageModel model) throws StorageException;

	Future<?> moveMessagesToJunk(String accountId, String sourceFolderId, Collection<String> messageIds,
								 String targetFolderId, boolean delete) throws NoSuchAccountException;

	Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
						   String targetFolderId, boolean delete) throws MessagingException, NoSuchAccountException;

	Future<?> markMessagesAsAnswered(String accountId, String folderId, Collection<String> messageIds, boolean flag) throws NoSuchAccountException;

	Future<?> markMessagesAsJunk(String accountId, String folderId, Collection<String> messageIds, boolean flag) throws NoSuchAccountException;

	Future<?> send(Model model) throws StorageException;
}