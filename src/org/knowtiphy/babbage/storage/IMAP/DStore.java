package org.knowtiphy.babbage.storage.IMAP;

import org.apache.commons.io.IOUtils;
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
import javax.mail.Part;
import javax.mail.UIDFolder;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
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
				.addL(folderId, Vocabulary.HAS_NAME, folder.getName());
	}

	static void addFolderCounts(Delta delta, IMAPAdapter account, Folder folder) throws MessagingException
	{
		String folderId = account.encode(folder);
		delta.addL(folderId, Vocabulary.HAS_MESSAGE_COUNT, folder.getMessageCount())
				.addL(folderId, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, folder.getUnreadMessageCount());
	}

	static void addMessage(Delta delta, String folderId, String messageId)
	{
		delta.addR(messageId, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE)
				.addR(folderId, Vocabulary.CONTAINS, messageId);
	}

	static void addMessageContent(Delta delta, IMAPAdapter adapter, Message message, MessageContent messageContent)
			throws MessagingException, IOException
	{
		String messageId = adapter.encode(message);
		delta.addL(messageId, Vocabulary.HAS_CONTENT, messageContent.getContent().getContent().toString())
				.addL(messageId, Vocabulary.HAS_MIME_TYPE, mimeType(messageContent.getContent()));
		for (Map.Entry<String, Part> entry : messageContent.getCidMap().entrySet())
		{
			String cidId = adapter.encode(message, entry.getKey());
			delta.addR(messageId, Vocabulary.HAS_CID_PART, cidId)
					.addL(cidId, Vocabulary.HAS_CONTENT, IOUtils.toByteArray(entry.getValue().getInputStream()))
					.addL(cidId, Vocabulary.HAS_MIME_TYPE, mimeType(entry.getValue()))
					.addL(cidId, Vocabulary.HAS_LOCAL_CID, entry.getKey());
		}

		int i = 0;
		for (Part part : messageContent.getAttachments())
		{
			String fileName = fileName(part);
			//  TODO -- what do we do if we have no filename?
			if (fileName != null)
			{
				String attachmentId = adapter.encode(message, String.valueOf(i));
				delta.addR(messageId, Vocabulary.HAS_ATTACHMENT, attachmentId)
						.addL(attachmentId, Vocabulary.HAS_CONTENT, IOUtils.toByteArray(part.getInputStream()))
						.addL(attachmentId, Vocabulary.HAS_MIME_TYPE, mimeType(part))
						.addL(attachmentId, Vocabulary.HAS_FILE_NAME, fileName);
				i++;
			}
		}
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
			if (org.knowtiphy.babbage.storage.IMAP.Pattern.MSG_JUNK_PATTERN.matcher(flag).matches())
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

	//  methods to access message content

	static String mimeType(Part part) throws MessagingException
	{
		return part.getContentType().split(";")[0];
	}

	static String fileName(Part part) throws MessagingException
	{
		String fileName = part.getFileName();
		if (fileName == null)
		{
			String[] headers = part.getHeader("Content-Type");
			if (headers != null)
			{
				for (String header : headers)
				{
					String[] split = header.split(";");
					for (String s : split)
					{
						String[] x = s.split("=");
						if (x.length == 2 && "name".equals(x[0].trim()))
						{
							String fn = x[1].trim();
							int start = fn.indexOf('"');
							int end = fn.lastIndexOf('"');
							fileName = fn.substring(start == -1 ? 0 : start + 1, end == -1 ? fn.length() : end);
						}
					}
				}
			}
		}

		return fileName;
	}
}