package org.knowtiphy.babbage.storage.IMAP;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.Address;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.UIDFolder;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;

import static org.knowtiphy.utils.JenaUtils.P;
import static org.knowtiphy.utils.JenaUtils.R;

/**
 * @author graham
 */
public interface DStore
{
	//	ADD methods -- these methods MUST only add triples to a model

	//	add an attribute (so subject predicate literal) triple as long as the attribute value is not null
	static <S, T> void addAttribute(Delta delta, String subject, String predicate, S value, Function<S, T> fn)
	{
		if (value != null)
		{
			delta.addDP(subject, predicate, fn.apply(value));
		}
	}

	static void addAddresses(Delta delta, String messageId, String predicate, Address[] addresses)
	{
		if (addresses != null)
		{
			for (Address address : addresses)
			{
				addAttribute(delta, messageId, predicate, address, Address::toString);
			}
		}
	}

	static void addFolder(Delta delta, IMAPAdapter account, Folder folder) throws MessagingException
	{
		String folderId = Encode.encode(folder);
		String type = account.specialId2Type.get(folderId);
		delta.addOP(folderId, RDF.type.toString(), type != null ? type : Vocabulary.IMAP_FOLDER)
				.addOP(account.getId(), Vocabulary.CONTAINS, folderId)
				.addDP(folderId, Vocabulary.HAS_UID_VALIDITY, ((UIDFolder) folder).getUIDValidity())
				.addDP(folderId, Vocabulary.HAS_NAME, folder.getName());
		//	remember that two calls to getFolder(X) can return different folder objects for the same folder
//				.addL(folderId, Vocabulary.IS_ARCHIVE_FOLDER, account.archive != null && folder.getName().equals(account.archive))
//				.addL(folderId, Vocabulary.IS_DRAFTS_FOLDER, account.drafts != null && folder.getName().equals(account.drafts))
//				.addL(folderId, Vocabulary.IS_INBOX, account.inbox != null && folder.getURLName().toString().equals(account.inbox))
//				.addL(folderId, Vocabulary.IS_JUNK_FOLDER, account.junk != null && folder.getName().equals(account.junk))
//				.addL(folderId, Vocabulary.IS_SENT_FOLDER, account.sent != null && folder.getName().equals(account.sent))
//				.addL(folderId, Vocabulary.IS_TRASH_FOLDER, account.trash != null && folder.getName().equals(account.trash));
	}

	static void addFolderCounts(Delta delta, Folder folder, String folderId) throws MessagingException
	{
		delta.addDP(folderId, Vocabulary.HAS_MESSAGE_COUNT, folder.getMessageCount())
				.addDP(folderId, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, folder.getUnreadMessageCount());
	}

	static void addMessage(Delta delta, String folderId, String messageId)
	{
		delta.addOP(messageId, RDF.type.toString(), Vocabulary.IMAP_MESSAGE)
				.addOP(folderId, Vocabulary.CONTAINS, messageId);
	}

	static void addMessageContent(Delta delta, Message message, String mid) throws Exception
	{
		var content = new MessageContent(message, mid,true).process();

		//System.out.println("addMessageContent -- BODY : " + content.id);
		delta.addDP(content.mid, Vocabulary.HAS_CONTENT, content.content)
				.addDP(content.mid, Vocabulary.HAS_MIME_TYPE, content.mimeType);
		//System.out.println("addMessageContent --- CIDs");
		for (InlineAttachment attachment : content.inlineAttachments)
		{
			//  Note: the local CID is a string, not a URI -- it is unique within a message, but not across messages
			delta.addOP(content.mid, Vocabulary.HAS_CID_PART, attachment.id)
					.addDP(attachment.id, Vocabulary.HAS_CONTENT, attachment.content)
					.addDP(attachment.id, Vocabulary.HAS_MIME_TYPE, attachment.mimeType)
					.addDP(attachment.id, Vocabulary.HAS_LOCAL_CID, attachment.localName);
		}

		//System.out.println("addMessageContent --- ATTACHMENTS");

		for (RegularAttachment attachment : content.regularAttachments)
		{
			//  TODO -- what do we do if we have no filename?
			if (attachment.fileName != null)
			{
				delta.addOP(content.mid, Vocabulary.HAS_ATTACHMENT, attachment.id)
						.addDP(attachment.id, Vocabulary.HAS_CONTENT, attachment.content)
						.addDP(attachment.id, Vocabulary.HAS_MIME_TYPE, attachment.mimeType)
						.addDP(attachment.id, Vocabulary.HAS_FILE_NAME, attachment.fileName);
			}
		}

		//System.out.println("addMessageContent --- END : " + content.id + " : " + (System.currentTimeMillis() - start));
	}

	static void addMessageFlags(Delta delta, Message message, String messageId) throws MessagingException
	{
		delta.addDP(messageId, Vocabulary.IS_READ, message.isSet(Flags.Flag.SEEN))
				.addDP(messageId, Vocabulary.IS_ANSWERED, message.isSet(Flags.Flag.ANSWERED));
		boolean junk = false;
		for (String flag : message.getFlags().getUserFlags())
		{
			//            if (MSG_NOT_JUNK_PATTERN.matcher(flag).matches())
			//            {
			//                junk = false;
			//                break;
			//            }
			if (Constants.MSG_JUNK_PATTERN.matcher(flag).matches())
			{
				junk = true;
			}
		}
		delta.addDP(messageId, Vocabulary.IS_JUNK, junk);
	}

	static void addMessageHeaders(Delta delta, Message message, String messageId) throws MessagingException
	{
		//System.out.println("addMessageHeaders " + messageId + " " + message.getSubject());
		addAttribute(delta, messageId, Vocabulary.HAS_SUBJECT, message.getSubject(), x -> x);
		addAttribute(delta, messageId, Vocabulary.RECEIVED_ON, message.getReceivedDate(),
				x -> new XSDDateTime(JenaUtils.fromDate(ZonedDateTime.ofInstant(x.toInstant(), ZoneId.systemDefault()))));
		addAttribute(delta, messageId, Vocabulary.SENT_ON, message.getSentDate(),
				x -> new XSDDateTime(JenaUtils.fromDate(ZonedDateTime.ofInstant(x.toInstant(), ZoneId.systemDefault()))));
		addAddresses(delta, messageId, Vocabulary.FROM, message.getFrom());
		addAddresses(delta, messageId, Vocabulary.TO, message.getRecipients(Message.RecipientType.TO));
		addAddresses(delta, messageId, Vocabulary.HAS_CC, message.getRecipients(Message.RecipientType.CC));
		addAddresses(delta, messageId, Vocabulary.HAS_BCC, message.getRecipients(Message.RecipientType.BCC));
		addMessageFlags(delta, message, messageId);
	}

	//	DELETE methods -- these methods MUST only add triples to a model (of deletes)

	static void deleteMessageFlags(Dataset dbase, Delta delta, String messageId)
	{
		var model = dbase.getDefaultModel();
		Resource mRes = R(model, messageId);
		delta.delete(model.listStatements(mRes, P(model, Vocabulary.IS_READ), (RDFNode) null))
				.delete(model.listStatements(mRes, P(model, Vocabulary.IS_ANSWERED), (RDFNode) null))
				.delete(model.listStatements(mRes, P(model, Vocabulary.IS_JUNK), (RDFNode) null));
	}

	static void deleteFolderCounts(Dataset dbase, Delta delta, String folderId)
	{
		var model = dbase.getDefaultModel();
		var fRes = R(model, folderId);
		delta.delete(model.listStatements(fRes, P(model, Vocabulary.HAS_MESSAGE_COUNT), (RDFNode) null))
				.delete(model.listStatements(fRes, P(model, Vocabulary.HAS_UNREAD_MESSAGE_COUNT),
						(RDFNode) null));
	}

	static void deleteFolder(Dataset dbase, Delta delta, String folderId)
	{
		Model model = dbase.getDefaultModel();
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource fRes = R(model, folderId);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = model.listStatements(fRes, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(model.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(model.listStatements(fRes, null, (RDFNode) null))
				.delete(model.listStatements(R(model, folderId), P(model, Vocabulary.CONTAINS), fRes));
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void deleteMessage(Dataset dbase, Delta delta, String folderId, String messageId)
	{
		Model model = dbase.getDefaultModel();
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource mRes = R(model, messageId);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = model.listStatements(mRes, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(model.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(model.listStatements(mRes, null, (RDFNode) null))
				.delete(model.listStatements(R(model, folderId), P(model, Vocabulary.CONTAINS), mRes));
	}
}


//
//	private SynchWork reconnectWork()
//	{
//		return new SynchWork(0)
//		{
//			@Override
//			public Void call() throws Exception
//			{
//				LOGGER.log(Level.INFO, "reconnectWork");
//				if (isDone())
//				{
//					System.err.println("------- NEW WORK SETUP :: reconnectWork FAILING -------");
//				}
//				else
//				{
//					reconnect();
//				}
//
//				return null;
//			}
//		};
//	}

//	private SynchWork reOpenFolderWork(final Folder folder)
//	{
//		return new SynchWork(0)
//		{
//			@Override
//			public Void call() throws Exception
//			{
//				LOGGER.log(Level.INFO, "reOpenFolder :: {0} :: {1}", new Object[]{folder.getName(), folder.isOpen()});
//				if (isDone())
//				{
//					System.err.println("------- NEW WORK SETUP :: reOpenFolder FAILING -------");
//				}
//				else
//				{
//					System.err.println("------- NEW WORK SETUP :: REOPENING FOLDER -------");
//					accountLock.lock();
//					try
//					{
//						folder.open(Folder.READ_WRITE);
//						m_PerFolderMessage.remove(folder);
//						synchMessageIdsAndHeaders(folder);
//					}
//					catch (IllegalStateException e)
//					{
//						//	the folder is already open
//					}
//					catch (MessagingException ex)
//					{
//						timesDone++;
//						addWork1(this, Constants.SYNCH_PRIORITY);
//					}
//					finally
//					{
//						accountLock.unlock();
//					}
//				}
//
//				return null;
//			}
//		};
//	}

//	private <T> void reschedule(SynchWork synchWork, int synchWorkPriority, MessageWork1 work, int workPriority)
//	{
//		addWork1(synchWork, synchWorkPriority);
//		addWork1(work, workPriority);
//	}

//	private abstract class Work implements Callable<Void>
//	{
//		int timesDone;
//
//		Work(int timesDone)
//		{
//			this.timesDone = timesDone;
//		}
//
//		public boolean isDone()
//		{
//			return timesDone == Constants.NUM_ATTEMPTS;
//		}
//	}

//	private abstract class SynchWork extends Work
//	{
//		SynchWork(int timesDone)
//		{
//			super(timesDone);
//		}
//	}

//	private class MessageWork1 extends Work
//	{
//		private final Callable<Void> work;
//
//		MessageWork1(int timesDone, Callable<Void> work)
//		{
//			super(timesDone);
//			this.work = work;
//		}
//
//		MessageWork1(Callable<Void> work)
//		{
//			this(0, work);
//		}
//
//		@Override
//		public Void call() throws Exception
//		{
//			try
//			{
//				timesDone++;
//				if (timesDone == Constants.NUM_ATTEMPTS)
//				{
//					System.err.println("------- NEW WORK SETUP :: MessageWork1 FAILING -------");
//					return null;
//				}
//				else
//				{
//					return work.call();
//				}
//			}
//			catch (MessageRemovedException ex)
//			{
//				System.err.println("------- NEW WORK SETUP :: MESSAGE DELETED -------");
//				LOGGER.log(Level.INFO, "MessageWork::message removed");
//				//	ignore
//			}
//			catch (StoreClosedException ex)
//			{
//				System.err.println("------- NEW WORK SETUP :: STORE CLOSED -------");
//				reschedule(reconnectWork(), Constants.SYNCH_PRIORITY,
//						new MessageWork1(timesDone, work), Constants.REDO_TASK + timesDone);
//			}
//			catch (FolderClosedException ex)
//			{
//				System.err.println("------- NEW WORK SETUP :: FOLDER CLOSED -------");
//				reschedule(reOpenFolderWork(ex.getFolder()), Constants.SYNCH_PRIORITY,
//						new MessageWork1(timesDone, work), Constants.REDO_TASK + timesDone);
//			}
//			catch (MailConnectException ex)
//			{
//				System.err.println("XXXXXXXXXXX TIMEOUT ISSUE XXXXXXXX");
//				//  TODO -- timeout -- not really sure what this does
//				LOGGER.info("TIMEOUT -- adapting timeout");
//				//timeout = timeout * 2;
//			}
//			catch (Exception ex)
//			{
//				System.err.println("XXXXXXXXXXX OTHER ISSUE XXXXXXXX");
//				//	usually a silly error where we did a dbase operation outside a transaction
//				LOGGER.info(() -> LoggerUtils.exceptionMessage(ex));
//				throw ex;
//			}
//
//			return null;
//		}
//	}

//	private class SynchWork implements Callable<Void>, HasPriority
//	{
//		private int priority;
//		private final Callable<Void> work;
//
//		SynchWork(int priority, Callable<Void> work)
//		{
//			this.priority = priority;
//			this.work = work;
//		}
//
//		@Override
//		public int getPriority()
//		{
//			return priority;
//		}
//
//		@Override
//		public Void call() throws Exception
//		{
////			try
////			{
//			return work.call();
//			///	}
////			catch (MessageRemovedException ex)
////			{
////				System.err.println("------- NEW WORK SETUP :: MESSAGE DELETED -------");
////				LOGGER.log(Level.INFO, "MessageWork::message removed");
////				//	ignore
////			}
////			catch (StoreClosedException ex)
////			{
////				System.err.println("------- NEW WORK SETUP :: STORE CLOSED -------");
////				reschedule(reconnectWork(), Constants.SYNCH_PRIORITY, this);
////			}
////			catch (FolderClosedException ex)
////			{
////				System.err.println("------- NEW WORK SETUP :: FOLDER CLOSED -------");
////				reschedule(reOpenFolderWork(ex.getFolder()), Constants.SYNCH_PRIORITY, this);
////			}
////			catch (MailConnectException ex)
////			{
////				System.err.println("XXXXXXXXXXX TIMEOUT ISSUE XXXXXXXX");
////				//  TODO -- timeout -- not really sure what this does
////				LOGGER.info("TIMEOUT -- adapting timeout");
////				//timeout = timeout * 2;
////			}
////			catch (Exception ex)
////			{
////				System.err.println("XXXXXXXXXXX OTHER ISSUE XXXXXXXX");
////				//	usually a silly error where we did a dbase operation outside a transaction
////				LOGGER.info(() -> LoggerUtils.exceptionMessage(ex));
////				throw ex;
////			}
////
////			return null;
//		}
//	}

//
//	private class Worker implements Runnable
//	{
//	{
//		private final BlockingQueue<? extends Runnable> queue;
//
//		Worker(BlockingQueue<? extends Runnable> queue)
//		{
//			this.queue = queue;
//		}
//
//		@Override
//		public void run()
//		{
//			while (true)
//			{
//				try
//				{
//					Runnable task = queue.take();
//					if (task == Constants.POISON_PILL)
//					{
//						return;
//					}
//					else
//					{
//						ensureMapsLoaded();
//						try
//						{
//							task.run();
//						} catch (RuntimeException ex)
//						{
//							LOGGER.warning(ex.getLocalizedMessage());
//						}
//					}
//				} catch (InterruptedException e)
//				{
//					return;
//				}
//			}
//		}
//	}