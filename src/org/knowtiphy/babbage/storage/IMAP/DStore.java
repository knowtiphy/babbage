package org.knowtiphy.babbage.storage.IMAP;

import org.apache.commons.io.IOUtils;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.IAdapter;
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
import java.util.regex.Pattern;

/**
 * @author graham
 */
public interface DStore
{
	Pattern MSG_JUNK_PATTERN = Pattern.compile("\\$?Junk", Pattern.CASE_INSENSITIVE);

	//	helper methods
	static Resource R(Model model, String uri)
	{
		return model.createResource(uri);
	}

	static Property P(Model model, String uri)
	{
		return model.createProperty(uri);
	}

	static <T> Literal L(Model model, T value)
	{
		return model.createTypedLiteral(value);
	}

	//	ADD methods -- these methods MUST only add triples to a model

	//	store a non-null attribute
	static <S> void addAttribute(Model model, Resource subject, String predicate, S value, Function<S, ? extends Literal> fn)
	{
		if (value != null)
		{
			model.add(subject, P(model, predicate), fn.apply(value));
		}
	}

	static void addAddresses(Model model, Resource messageId, String predicate, Address[] addresses)
	{
		if (addresses != null)
		{
			for (Address address : addresses)
			{
				assert address != null;
				model.add(messageId, P(model, predicate), L(model, address.toString()));
			}
		}
	}

	static void addFolderCounts(Model model, IAdapter account, Folder folder) throws MessagingException
	{
		Resource fRes = R(model, account.encode(folder));
		model.add(fRes, P(model, Vocabulary.HAS_MESSAGE_COUNT), L(model, folder.getMessageCount()));
		model.add(fRes, P(model, Vocabulary.HAS_UNREAD_MESSAGE_COUNT), L(model, folder.getUnreadMessageCount()));
	}

	static void addMessageID(Model model, String folderId, String messageId)
	{
		Resource mRes = R(model, messageId);
		model.add(mRes, P(model, Vocabulary.RDF_TYPE), R(model, Vocabulary.IMAP_MESSAGE));
		model.add(R(model, folderId), P(model, Vocabulary.CONTAINS), mRes);
	}

	static void addMessageContent(Model model, IMAPAdapter IMAPAdapter, Message message, MessageContent messageContent) throws MessagingException, IOException
	{
		Resource mRes = model.createResource(IMAPAdapter.encode(message));
		model.add(mRes, P(model, Vocabulary.HAS_CONTENT), L(model, messageContent.getContent().getContent().toString()));
		model.add(mRes, P(model, Vocabulary.HAS_MIME_TYPE), L(model, mimeType(messageContent.getContent())));
		for (Map.Entry<String, Part> entry : messageContent.getCidMap().entrySet())
		{
			Resource cidRes = R(model, IMAPAdapter.encode(message, entry.getKey()));
			model.add(mRes, P(model, Vocabulary.HAS_CID_PART), cidRes);
			model.add(cidRes, P(model, Vocabulary.HAS_CONTENT), L(model, IOUtils.toByteArray(entry.getValue().getInputStream())));
			model.add(cidRes, P(model, Vocabulary.HAS_MIME_TYPE), L(model, mimeType(entry.getValue())));
			model.add(cidRes, P(model, Vocabulary.HAS_LOCAL_CID), R(model, entry.getKey()));
		}

		int i = 0;
		for (Part part : messageContent.getAttachments())
		{
			String fileName = fileName(part);
			//  TODO -- what do we do if we have no filename?
			if (fileName != null)
			{
				Resource aRes = R(model, IMAPAdapter.encode(message, String.valueOf(i)));
				model.add(mRes, P(model, Vocabulary.HAS_ATTACHMENT), aRes);
				model.add(aRes, P(model, Vocabulary.HAS_CONTENT), L(model, IOUtils.toByteArray(part.getInputStream())));
				model.add(aRes, P(model, Vocabulary.HAS_MIME_TYPE), L(model, mimeType(part)));
				model.add(aRes, P(model, Vocabulary.HAS_FILE_NAME), L(model, fileName));
				i++;
			}
		}
	}

	static void addFlags(Model model, String messageId, Message message) throws MessagingException
	{
		Resource mRes = R(model, messageId);
		model.add(mRes, P(model, Vocabulary.IS_READ), L(model, message.isSet(Flags.Flag.SEEN)));
		model.add(mRes, P(model, Vocabulary.IS_ANSWERED), L(model, message.isSet(Flags.Flag.ANSWERED)));
		boolean junk = false;
		for (String flag : message.getFlags().getUserFlags())
		{
			//            if (MSG_NOT_JUNK_PATTERN.matcher(flag).matches())
			//            {
			//                junk = false;
			//                break;
			//            }
			if (MSG_JUNK_PATTERN.matcher(flag).matches())
			{
				junk = true;
			}
		}
		model.add(mRes, P(model, Vocabulary.IS_JUNK), L(model, junk));
	}

	static void addFolder(Model model, IAdapter account, Folder folder) throws MessagingException
	{
		Resource fRes = R(model, account.encode(folder));
		model.add(fRes, model.createProperty(Vocabulary.RDF_TYPE), model.createResource(Vocabulary.IMAP_FOLDER));
		model.add(R(model, account.getId()), P(model, Vocabulary.CONTAINS), fRes);
		model.add(fRes, P(model, Vocabulary.HAS_UID_VALIDITY), L(model, ((UIDFolder) folder).getUIDValidity()));
		model.add(fRes, P(model, Vocabulary.HAS_NAME), L(model, folder.getName()));
	}

	static void addMessageHeaders(Model model, Message message, String messageId) throws MessagingException
	{
		Resource messageRes = R(model, messageId);
		addAttribute(model, messageRes, Vocabulary.HAS_SUBJECT, message.getSubject(), x -> L(model, x));
		addAttribute(model, messageRes, Vocabulary.RECEIVED_ON, message.getReceivedDate(),
				x -> L(model, new XSDDateTime(JenaUtils.fromDate(ZonedDateTime.ofInstant(x.toInstant(), ZoneId.systemDefault())))));
		addAttribute(model, messageRes, Vocabulary.SENT_ON, message.getSentDate(),
				x -> L(model, new XSDDateTime(JenaUtils.fromDate(ZonedDateTime.ofInstant(x.toInstant(), ZoneId.systemDefault())))));
		addAddresses(model, messageRes, Vocabulary.FROM, message.getFrom());
		addAddresses(model, messageRes, Vocabulary.TO, message.getRecipients(Message.RecipientType.TO));
		addAddresses(model, messageRes, Vocabulary.HAS_CC, message.getRecipients(Message.RecipientType.CC));
		addAddresses(model, messageRes, Vocabulary.HAS_BCC, message.getRecipients(Message.RecipientType.BCC));
		addFlags(model, messageId, message);
	}

	//	DELETE methods -- these methods MUST only add triples to a model (of deletes)

	static void deleteMessageFlags(Model dbase, Model deletes, String messageId)
	{
		Resource mRes = R(deletes, messageId);
		deletes.add(dbase.listStatements(mRes, P(dbase, Vocabulary.IS_READ), (RDFNode) null));
		deletes.add(dbase.listStatements(mRes, P(dbase, Vocabulary.IS_ANSWERED), (RDFNode) null));
		deletes.add(dbase.listStatements(mRes, P(dbase, Vocabulary.IS_JUNK), (RDFNode) null));
	}

	static void deleteFolderCounts(Model dbase, Model deletes, String folderId)
	{
		Resource fRes = R(dbase, folderId);
		deletes.add(dbase.listStatements(fRes, P(dbase, Vocabulary.HAS_MESSAGE_COUNT), (RDFNode) null));
		deletes.add(dbase.listStatements(fRes, P(dbase, Vocabulary.HAS_UNREAD_MESSAGE_COUNT), (RDFNode) null));
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void deleteMessage(Model dbase, Model deletes, String folderId, String messageId)
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
				deletes.add(dbase.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		deletes.add(dbase.listStatements(mRes, null, (RDFNode) null));
		deletes.add(dbase.listStatements(R(dbase, folderId), P(dbase, Vocabulary.CONTAINS), mRes));
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