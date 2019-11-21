package org.knowtiphy.babbage.storage.IMAP;

import biweekly.component.VEvent;
import org.apache.commons.io.IOUtils;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.*;
import org.knowtiphy.babbage.storage.IAdapter;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import javax.mail.*;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author graham
 */
public interface DStore
{
	Pattern MSG_JUNK_PATTERN = Pattern.compile("\\$?Junk", Pattern.CASE_INSENSITIVE);

	static Resource R(Model model, String name)
	{
		return model.createResource(name);
	}

	static Property P(ModelCon model, String name)
	{
		return model.createProperty(name);
	}

	static <T> Literal L(ModelCon model, T value)
	{
		return model.createTypedLiteral(value);
	}

	static void addresses(ModelCon model, Resource messageName, String property, Address[] addresses)
	{
		if (addresses != null)
		{
			for (Address address : addresses)
			{
				assert address != null;
				model.add(messageName, P(model, property), L(model, address.toString()));
			}
		}
	}

	static <S> void attr(Model model, Resource subject, String predicate, S value, Function<S, ? extends Literal> fn)
	{
		if (value != null)
		{
			model.add(subject, P(model, predicate), fn.apply(value));
		}
	}

	//  AUDIT -- ok, because it removes and adds
	static void folderCounts(Model model, IAdapter account, Folder folder) throws MessagingException
	{
		Resource messageRes = R(model, account.encode(folder));
		model.remove(model.listStatements(messageRes, P(model, Vocabulary.HAS_MESSAGE_COUNT), (RDFNode) null));
		model.remove(model.listStatements(messageRes, P(model, Vocabulary.HAS_UNREAD_MESSAGE_COUNT), (RDFNode) null));
		model.add(messageRes, P(model, Vocabulary.HAS_MESSAGE_COUNT), L(model, folder.getMessageCount()));
		model.add(messageRes, P(model, Vocabulary.HAS_UNREAD_MESSAGE_COUNT), L(model, folder.getUnreadMessageCount()));
	}

	static void flags(Model model, String messageName, Message message) throws MessagingException
	{
		Resource messageRes = R(model, messageName);
		model.remove(model.listStatements(messageRes, P(model, Vocabulary.IS_READ), (RDFNode) null));
		model.remove(model.listStatements(messageRes, P(model, Vocabulary.IS_ANSWERED), (RDFNode) null));
		model.remove(model.listStatements(messageRes, P(model, Vocabulary.IS_JUNK), (RDFNode) null));
		model.add(messageRes, P(model, Vocabulary.IS_READ), L(model, message.isSet(Flags.Flag.SEEN)));
		model.add(messageRes, P(model, Vocabulary.IS_ANSWERED), L(model, message.isSet(Flags.Flag.ANSWERED)));
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
		//System.err.println("IS JUNK " + Arrays.toString(message.getFlags().getUserFlags()) + "  :: " + junk);
		model.add(messageRes, P(model, Vocabulary.IS_JUNK), L(model, junk));
	}

	static void messageHeaders(Model model, Message message, String messageName) throws MessagingException
	{
		Resource messageRes = R(model, messageName);
		attr(model, messageRes, Vocabulary.HAS_SUBJECT, message.getSubject(), x -> L(model, x));
		attr(model, messageRes, Vocabulary.RECEIVED_ON, message.getReceivedDate(), x ->
		{
			if (x == null)
			{
				System.out.println("DATE IS NULL");
			}
			return L(model, new XSDDateTime(JenaUtils.fromDate(x)));
		});
		attr(model, messageRes, Vocabulary.SENT_ON, message.getSentDate(), x -> L(model, new XSDDateTime(JenaUtils.fromDate(x))));
		addresses(model, messageRes, Vocabulary.FROM, message.getFrom());
		addresses(model, messageRes, Vocabulary.TO, message.getRecipients(Message.RecipientType.TO));
		addresses(model, messageRes, Vocabulary.HAS_CC, message.getRecipients(Message.RecipientType.CC));
		addresses(model, messageRes, Vocabulary.HAS_BCC, message.getRecipients(Message.RecipientType.BCC));
		flags(model, messageName, message);
	}

	static void messageID(Model model, String folderName, String messageName)
	{
		Resource messageRes = R(model, messageName);
		model.add(messageRes, P(model, Vocabulary.RDF_TYPE), model.createResource(Vocabulary.IMAP_MESSAGE));
		model.add(R(model, folderName), P(model, Vocabulary.CONTAINS), messageRes);
	}

	static void event(Model model, String calendarName, String eventName, VEvent event)
	{
		Resource eventRes = R(model, eventName);
		model.add(eventRes, P(model, Vocabulary.RDF_TYPE), model.createResource(Vocabulary.CALDAV_EVENT));
		model.add(R(model, calendarName), P(model, Vocabulary.CONTAINS), eventRes);

		attr(model, eventRes, Vocabulary.HAS_SUMMARY, event.getSummary(), x -> L(model, x));
		attr(model, eventRes, Vocabulary.HAS_DATE_START, event.getDateStart(), x -> L(model, new XSDDateTime(JenaUtils.fromDate(x))));
		attr(model, eventRes, Vocabulary.HAS_DATE_END, event.getDateEnd(), x -> L(model, new XSDDateTime(JenaUtils.fromDate(x))));
		attr(model, eventRes, Vocabulary.HAS_DESCRIPTION, event.getDescription(), x -> L(model, x));
		attr(model, eventRes, Vocabulary.HAS_PRIORITY, event.getPriority(), x -> L(model, x));

	}

//    static void outGoingMessageId(Model model, String accountId, String messageId)
//    {
//        Resource messageRes = R(model, messageId);
//        model.add(messageRes, P(model, Vocabulary.RDF_TYPE), model.createResource(Vocabulary.DRAFT_MESSAGE));
//        model.add(R(model, accountId), P(model, Vocabulary.CONTAINS), messageRes);
//    }

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

	static void messageContent(Model model, IMAPAdapter IMAPAdapter, Message message, MessageContent messageContent) throws MessagingException, IOException
	{
		Resource messageRes = model.createResource(IMAPAdapter.encode(message));
		model.add(messageRes, P(model, Vocabulary.HAS_CONTENT), L(model, messageContent.getContent().getContent().toString()));
		model.add(messageRes, P(model, Vocabulary.HAS_MIME_TYPE), L(model, mimeType(messageContent.getContent())));
		for (Map.Entry<String, Part> entry : messageContent.getCidMap().entrySet())
		{
			Resource cidRes = R(model, IMAPAdapter.encode(message, entry.getKey()));
			model.add(messageRes, P(model, Vocabulary.HAS_CID_PART), cidRes);
			model.add(cidRes, P(model, Vocabulary.HAS_CONTENT), model.createTypedLiteral(IOUtils.toByteArray(entry.getValue().getInputStream())));
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
				Resource attachRes = R(model, IMAPAdapter.encode(message, String.valueOf(i)));
				model.add(messageRes, P(model, Vocabulary.HAS_ATTACHMENT), attachRes);
				model.add(attachRes, P(model, Vocabulary.HAS_CONTENT), model.createTypedLiteral(IOUtils.toByteArray(part.getInputStream())));
				model.add(attachRes, P(model, Vocabulary.HAS_MIME_TYPE), L(model, mimeType(part)));
				model.add(attachRes, P(model, Vocabulary.HAS_FILE_NAME), L(model, fileName));
				i++;
			}
		}
	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreMessage(Model model, String folderName, String messageName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource messageRes = R(model, messageName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = model.listStatements(messageRes, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				model.remove(model.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		model.remove(model.listStatements(messageRes, null, (RDFNode) null));
		model.remove(model.listStatements(R(model, folderName), P(model, Vocabulary.CONTAINS), messageRes));
	}

//    static void unstoreDraft(Model model, String messageId) throws MessagingException
//    {
//        System.err.println("DELETING D(" + messageId + ")");
//        Resource messageRes = R(model, messageId);
//        //  TODO -- delete everything reachable from messageName
//        StmtIterator it = model.listStatements(messageRes, null, (RDFNode) null);
//        while (it.hasNext())
//        {
//            Statement stmt = it.next();
//            if (stmt.getObject().isResource())
//            {
//                model.remove(model.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
//            }
//        }
//    }
}