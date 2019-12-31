package org.knowtiphy.babbage.storage.IMAP;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
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

/**
 * @author graham
 */
public interface DStore
{
	//	helper methods
	static Resource R(Model model, String uri)
	{
		return model.createResource(uri);
	}

	static Property P(Model model, String uri)
	{
		return model.createProperty(uri);
	}

	//	ADD methods -- these methods MUST only add triples to a model

	//	add an attribute (so subject predicate literal) triple as long as the attribute value is not null
	static <S, T> void addAttribute(Delta delta, String subject, String predicate, S value, Function<S, T> fn)
	{
		if (value != null)
		{
			delta.addL(subject, predicate, fn.apply(value));
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
		String folderId = account.encode(folder);
		delta.addR(folderId, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER)
				.addR(account.getId(), Vocabulary.CONTAINS, folderId)
				.addL(folderId, Vocabulary.HAS_UID_VALIDITY, ((UIDFolder) folder).getUIDValidity())
				.addL(folderId, Vocabulary.HAS_NAME, folder.getName())
				//	remember that two calls to getFolder(X) can return different folder objects for the same folder
				.addL(folderId, Vocabulary.IS_ARCHIVE_FOLDER, account.archive != null && folder.getName().equals(account.archive.getName()))
				.addL(folderId, Vocabulary.IS_DRAFTS_FOLDER, account.drafts != null && folder.getName().equals(account.drafts.getName()))
				.addL(folderId, Vocabulary.IS_INBOX, account.inbox != null && folder.getName().equals(account.inbox.getName()))
				.addL(folderId, Vocabulary.IS_JUNK_FOLDER, account.junk != null && folder.getName().equals(account.junk.getName()))
				.addL(folderId, Vocabulary.IS_SENT_FOLDER, account.sent != null && folder.getName().equals(account.sent.getName()))
				.addL(folderId, Vocabulary.IS_TRASH_FOLDER, account.trash != null && folder.getName().equals(account.trash.getName()));
	}

	static void addFolderCounts(Delta delta, Folder folder, String folderId) throws MessagingException
	{
		delta.addL(folderId, Vocabulary.HAS_MESSAGE_COUNT, folder.getMessageCount())
				.addL(folderId, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, folder.getUnreadMessageCount());
	}

	static void addMessage(Delta delta, String folderId, String messageId)
	{
		delta.addR(messageId, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE)
				.addR(folderId, Vocabulary.CONTAINS, messageId);
	}

	static void addMessageContent(Delta delta, MessageContent content)
	{
		long start = System.currentTimeMillis();
		System.out.println("addMessageContent -- BODY : " + content.id);
		delta.addL(content.id, Vocabulary.HAS_CONTENT, content.content)
				.addL(content.id, Vocabulary.HAS_MIME_TYPE, content.mimeType);
		System.out.println("addMessageContent --- CIDs");
		for (InlineAttachment attachment : content.inlineAttachments)
		{
			//  Note: the local CID is a string, not a URI -- it is unique within a message, but not across messages
			delta.addR(content.id, Vocabulary.HAS_CID_PART, attachment.id)
					.addL(attachment.id, Vocabulary.HAS_CONTENT, attachment.content)
					.addL(attachment.id, Vocabulary.HAS_MIME_TYPE, attachment.mimeType)
					.addL(attachment.id, Vocabulary.HAS_LOCAL_CID, attachment.localName);
		}

		System.out.println("addMessageContent --- ATTACHMENTS");

		for (RegularAttachment attachment : content.regularAttachments)
		{
			//  TODO -- what do we do if we have no filename?
			if (attachment.fileName != null)
			{
				delta.addR(content.id, Vocabulary.HAS_ATTACHMENT, attachment.id)
						.addL(attachment.id, Vocabulary.HAS_CONTENT, attachment.content)
						.addL(attachment.id, Vocabulary.HAS_MIME_TYPE, attachment.mimeType)
						.addL(attachment.id, Vocabulary.HAS_FILE_NAME, attachment.fileName);
			}
		}

		System.out.println("addMessageContent --- END : " + content.id + " : " + (System.currentTimeMillis() - start));
	}

	static void addMessageFlags(Delta delta, Message message, String messageId) throws MessagingException
	{
		delta.addL(messageId, Vocabulary.IS_READ, message.isSet(Flags.Flag.SEEN))
				.addL(messageId, Vocabulary.IS_ANSWERED, message.isSet(Flags.Flag.ANSWERED));
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
		delta.addL(messageId, Vocabulary.IS_JUNK, junk);
	}

	static void addMessageHeaders(Delta delta, Message message, String messageId) throws MessagingException
	{
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

	static void deleteMessageFlags(Model dbase, Delta delta, String messageId)
	{
		Resource mRes = R(dbase, messageId);
		delta.delete(dbase.listStatements(mRes, P(dbase, Vocabulary.IS_READ), (RDFNode) null))
				.delete(dbase.listStatements(mRes, P(dbase, Vocabulary.IS_ANSWERED), (RDFNode) null))
				.delete(dbase.listStatements(mRes, P(dbase, Vocabulary.IS_JUNK), (RDFNode) null));
	}

	static void deleteFolderCounts(Model dbase, Delta delta, String folderId)
	{
		Resource fRes = R(dbase, folderId);
		delta.delete(dbase.listStatements(fRes, P(dbase, Vocabulary.HAS_MESSAGE_COUNT), (RDFNode) null))
				.delete(dbase.listStatements(fRes, P(dbase, Vocabulary.HAS_UNREAD_MESSAGE_COUNT), (RDFNode) null));
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void deleteMessage(Model dbase, Delta delta, String folderId, String messageId)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource mRes = R(dbase, messageId);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = dbase.listStatements(mRes, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(dbase.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(dbase.listStatements(mRes, null, (RDFNode) null))
				.delete(dbase.listStatements(R(dbase, folderId), P(dbase, Vocabulary.CONTAINS), mRes));
	}

	//	methods to update stuff -- delete then add

	static void updateFolderCounts(Model dbase, Delta delta, Folder folder, String folderId) throws MessagingException
	{
		DStore.deleteFolderCounts(dbase, delta, folderId);
		DStore.addFolderCounts(delta, folder, folderId);
	}
}