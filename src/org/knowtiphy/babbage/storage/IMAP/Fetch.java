package org.knowtiphy.babbage.storage.IMAP;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;

import static org.knowtiphy.babbage.storage.IMAP.DStore.P;
import static org.knowtiphy.babbage.storage.IMAP.DStore.R;

/**
 * @author graham
 */
public interface Fetch
{
	//private final static Pattern MSG_NOT_JUNK_PATTERN = Pattern.compile("(\\$NotJunk)", Pattern.CASE_INSENSITIVE);
	static String skeleton(String id)
	{
		return String.format("CONSTRUCT { ?%s <%s> <%s> . " +
						"    <%s> <%s> ?%s  . " +
						" ?%s <%s> ?%s   . " +
						" ?%s <%s> ?%s . " +
						"?%s <%s> ?%s}\n" +
						"WHERE \n" + "{\n" + " ?%s <%s> <%s>.\n" + "      "
						+ "                 <%s> <%s> ?%s.\n"
						+ "                ?%s <%s> ?%s.\n" + "      "
						+ "                ?%s <%s> ?%s.\n" + "      "
						+ "                ?%s <%s> ?%s.\n" + "}",
				// START OF CONSTRUCT
				Vars.VAR_FOLDER_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER,
				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT,
				// START OF WHERE
				Vars.VAR_FOLDER_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER,
				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT);
	}

	static String initialState(String id)
	{
		return String.format("CONSTRUCT { ?%s <%s> ?%s . "
						+ " ?%s <%s> <%s> . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ "?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s . "
						+ " ?%s <%s> ?%s }\n"
						+ "WHERE \n" + "{\n" + "      "
						+ " ?%s <%s> ?%s.\n" + "      "
						+ " <%s>  <%s> ?%s.\n" + "      "
						+ " ?%s  <%s> <%s>.\n"
						+ " ?%s  <%s> ?%s.\n" + "      "
						+ " ?%s  <%s> ?%s.\n" + "      "
						+ " ?%s  <%s> ?%s.\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n" + "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n" + "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n" + "      OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "      OPTIONAL { ?%s  <%s> ?%s }\n" + "}",
				// START OF CONSTRUCT
				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
				Vars.VAR_MESSAGE_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_READ, Vars.VAR_IS_READ,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_JUNK, Vars.VAR_IS_JUNK,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_ANSWERED, Vars.VAR_IS_ANSWERED,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_SUBJECT, Vars.VAR_SUBJECT,
				Vars.VAR_MESSAGE_ID, Vocabulary.RECEIVED_ON, Vars.VAR_RECEIVED_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.SENT_ON, Vars.VAR_SENT_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.TO, Vars.VAR_TO,
				Vars.VAR_MESSAGE_ID, Vocabulary.FROM, Vars.VAR_FROM,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_CC, Vars.VAR_CC,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_BCC, Vars.VAR_BCC,
				// START OF WHERE
				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
				Vars.VAR_MESSAGE_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_READ, Vars.VAR_IS_READ,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_JUNK, Vars.VAR_IS_JUNK,
				Vars.VAR_MESSAGE_ID, Vocabulary.IS_ANSWERED, Vars.VAR_IS_ANSWERED,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_SUBJECT, Vars.VAR_SUBJECT,
				Vars.VAR_MESSAGE_ID, Vocabulary.RECEIVED_ON, Vars.VAR_RECEIVED_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.SENT_ON, Vars.VAR_SENT_ON,
				Vars.VAR_MESSAGE_ID, Vocabulary.TO, Vars.VAR_TO,
				Vars.VAR_MESSAGE_ID, Vocabulary.FROM, Vars.VAR_FROM,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_CC, Vars.VAR_CC,
				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_BCC, Vars.VAR_BCC);
	}

	static StmtIterator folder(Model model, String folderId)
	{
		return model.listStatements(model.createResource(folderId), P(model, Vocabulary.RDF_TYPE), R(model, Vocabulary.IMAP_FOLDER));
	}

	static String messageUIDs(String folderId)
	{
		return "SELECT ?message "
				+ "WHERE {"
				+ "      ?message <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.IMAP_MESSAGE + ">.\n"
				+ "      <" + folderId + "> <" + Vocabulary.CONTAINS + "> ?message.\n"
				+ "      }";
	}

	static String eventURIs(String calURI)
	{
		return "SELECT ?event "
				+ "WHERE {"
				+ "      ?event <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.CALDAV_EVENT + ">.\n"
				+ "      <" + calURI + "> <" + Vocabulary.CONTAINS + "> ?event.\n"
				+ "      }";
	}

	static String messageUIDsWithHeaders(String accountId, String folderId)
	{
		return "SELECT ?message\n"
				+ "WHERE \n"
				+ "{\n"
				+ "      ?message <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.IMAP_MESSAGE + ">.\n"
				+ "      <" + folderId + "> <" + Vocabulary.CONTAINS + "> ?message.\n"
				+ "      <" + accountId + "> <" + Vocabulary.CONTAINS + "> <" + folderId + ">.\n"
				//  if a message has an IS_READ then we have headers for it
				+ "      ?message <" + Vocabulary.IS_READ + "> ?isRead.\n"
				+ "}";
	}

	static String outboxMessage(String accountId, String messageId)
	{
		return "SELECT ?subject ?content ?to ?cc ?bcc\n"
				+ "WHERE \n"
				+ "{\n"
				+ "      <" + messageId + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.DRAFT_MESSAGE + ">.\n"
				+ "      <" + accountId + "> <" + Vocabulary.CONTAINS + "> <" + messageId + ">.\n"
				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_SUBJECT + "> ?subject }\n"
				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_CONTENT + "> ?content } \n"
				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.TO + "> ?to } \n"
				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_CC + "> ?cc } \n"
				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_BCC + "> ?bcc } \n"
				+ "}";
	}

	static String outboxMessageIds(String accountId)
	{
		return "SELECT ?messageId\n"
				+ "WHERE \n"
				+ "{\n"
				+ "      ?messageId <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.DRAFT_MESSAGE + ">.\n"
				+ "      <" + accountId + "> <" + Vocabulary.CONTAINS + "> ?messageId.\n"
				+ "}";
	}
}