/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knowtiphy.babbage.storage;

import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.IMAP.MessageModel;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author graham
 */

public interface IAdapter
{
	String getId();

	String getType();

	void addListener();

	void initialize(Delta delta) throws Exception;

	//void addInitialTriples(Delta delta);

	void close(Model model);

	Model sync() throws ExecutionException, InterruptedException;

	Future<?> sync(String fid) throws ExecutionException, InterruptedException;

	Future<?> doOperation(String oid, String type, Model operation);

	//	all this code has to go away

	ResultSet query(String query);

	Future<?> markMessagesAsAnswered(Collection<String> messageIds, String folderId, boolean flag);

	Future<?> markMessagesAsJunk(Collection<String> messageIds, String folderId, boolean flag);

	Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
								 boolean delete);

	Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId, boolean delete) throws MessagingException;

	Future<?> appendMessages(String folderId, Message[] messages);

	Message createMessage(MessageModel model) throws MessagingException, IOException;

	Future<?> ensureMessageContentLoaded(String messageId, String folderId);

	Future<?> loadAhead(String folderId, Collection<String> messageIds);

	Future<?> send(Model model);
}