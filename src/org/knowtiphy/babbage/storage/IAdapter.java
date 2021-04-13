/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knowtiphy.babbage.storage;

import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.exceptions.NoOperationSpecifiedException;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * @author graham
 */

public interface IAdapter
{
	String getId();

	String getType();

	void initialize(Map<String, Function<String, Future<?>>> syncs, Delta delta) throws Exception;

	void close(Model model);

	Future<?> doOperation(String oid, String type, Model operation) throws NoOperationSpecifiedException;

	//	all this code has to go away

	//Future<?> sync(String fid) throws ExecutionException, InterruptedException;

	Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								 boolean delete);

	Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId, boolean delete) throws MessagingException;

	Future<?> appendMessages(String folderId, Message[] messages);
}