/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knowtiphy.babbage.storage;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 *
 * @author graham
 */

// TODO: Will likely need to make this IAdapter and change around the methods, or just add more
public interface IAdapter
{

	String getId();

	String encode(Folder folder) throws MessagingException;

	void close();

	Future<?> markMessagesAsAnswered(Collection<String> messageIds, String folderId, boolean flag);

	Future<?> markMessagesAsRead(Collection<String> messageIds, String folderId, boolean flag);

	Future<?> markMessagesAsJunk(Collection<String> messageIds, String folderId, boolean flag);

	Future<?> moveMessagesToJunk(String sourceFolderId, Collection<String> messageIds, String targetFolderId,
			boolean delete);

	Future<?> copyMessages(String sourceFolderId, Collection<String> messageIds, String targetFolderId, boolean delete);

	Future<?> deleteMessages(String folderId, Collection<String> messageIds);

	Future<?> appendMessages(String folderId, Message[] messages);

	Message createMessage(MessageModel model) throws MessagingException, IOException;

	FutureTask<?> getSynchTask();

	Future<?> ensureMessageContentLoaded(String messageId, String folderId);

	//	public default boolean equals(IAccount acc)
//	{
//		return this.getId().equals(acc.getId());
//	}

	/*String getServerName();

	String getEmailAddress();

	String getPassword();

	String encode(Folder folder) throws MessagingException;*/
}
