package org.knowtiphy.babbage.storage;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.exceptions.NoSuchAccountException;
import org.knowtiphy.babbage.storage.exceptions.StorageException;

import javax.mail.MessagingException;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * @author graham
 */
public interface IStorage extends AutoCloseable
{
	void addListener(IStorageListener listener) throws StorageException;

	void close();

	Future<?> doOperation(Model operation) throws StorageException;

	//	note -- this method on the server closes the result set from running the query and
	//	returns a copy of that result set -- presamably its in mem? does the copy require closing?
	ResultSet query(String query) throws StorageException;

	//	For time being, stick all extra methods in here
	//	Most should be done via doOp

	Model sync(String id) throws StorageException;

	Future<?> sync(String id, String fid) throws StorageException;

	Future<?> moveMessagesToJunk(String accountId, String sourceFolderId, Collection<String> messageIds,
								 String targetFolderId, boolean delete) throws NoSuchAccountException;

	Future<?> copyMessages(String accountId, String sourceFolderId, Collection<String> messageIds,
						   String targetFolderId, boolean delete) throws MessagingException, NoSuchAccountException;
}