package org.knowtiphy.babbage.storage.IMAP;

import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Vars;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

import java.util.HashSet;
import java.util.Set;

import static org.knowtiphy.babbage.storage.IMAP.DStore.P;
import static org.knowtiphy.babbage.storage.IMAP.DStore.R;

/**
 * @author graham
 */
public interface DFetch
{
	static String skeleton(String id)
	{
		return String.format("CONSTRUCT\n"
						+ "{\n"
						+ "?%s <%s> <%s>.\n"
						+ "<%s> <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s\n"
						+ "}\n"
						+ "WHERE\n"
						+ "{\n"
						+ " ?%s <%s> <%s>.\n"
						+ "<%s> <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "}",
				// START OF CONSTRUCT
				Vars.VAR_FOLDER_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER,
				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT,
				Vars.VAR_FOLDER_ID, Vocabulary.IS_INBOX, Vars.VAR_IS_INBOX_FOLDER,
				Vars.VAR_FOLDER_ID, Vocabulary.IS_TRASH_FOLDER, Vars.VAR_IS_TRASH_FOLDER,
				Vars.VAR_FOLDER_ID, Vocabulary.IS_JUNK_FOLDER, Vars.VAR_IS_JUNK_FOLDER,
				// START OF WHERE
				Vars.VAR_FOLDER_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_FOLDER,
				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
				Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT,
				Vars.VAR_FOLDER_ID, Vocabulary.IS_INBOX, Vars.VAR_IS_INBOX_FOLDER,
				Vars.VAR_FOLDER_ID, Vocabulary.IS_TRASH_FOLDER, Vars.VAR_IS_TRASH_FOLDER,
				Vars.VAR_FOLDER_ID, Vocabulary.IS_JUNK_FOLDER, Vars.VAR_IS_JUNK_FOLDER);
	}

	static String initialState(String id)
	{
		return String.format("CONSTRUCT\n" +
						"{\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> <%s>.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s.\n"
						+ "?%s <%s> ?%s }\n"
						+ "WHERE\n"
						+ "{\n" + "      "
						+ "?%s <%s> ?%s.\n" + "      "
						+ "<%s>  <%s> ?%s.\n" + "      "
						+ "?%s  <%s> <%s>.\n"
						+ "?%s  <%s> ?%s.\n" + "      "
						+ "?%s  <%s> ?%s.\n" + "      "
						+ "?%s  <%s> ?%s.\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
						+ "}",
				// START OF CONSTRUCT
				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
				Vars.VAR_MESSAGE_ID, Vocabulary.RDF_TYPE, Vocabulary.IMAP_MESSAGE,
				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
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
		return model.listStatements(R(model, folderId), P(model, Vocabulary.RDF_TYPE), R(model, Vocabulary.IMAP_FOLDER));
	}

	static String messageUIDs(String folderId)
	{
		return "SELECT ?message "
				+ "WHERE {"
				+ "      ?message <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.IMAP_MESSAGE + ">.\n"
				+ "      <" + folderId + "> <" + Vocabulary.CONTAINS + "> ?message.\n"
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

	//  get the ids messages stored in the database that match the query
	public static Set<String> messageIds(Model model, String query)
	{
		Set<String> stored = new HashSet<>(1000);
		ResultSet resultSet = QueryExecutionFactory.create(query, model).execSelect();
		stored.addAll(JenaUtils.set(resultSet, soln -> soln.get("message").asResource().toString()));
		return stored;
	}

//
//	static String outboxMessage(String accountId, String messageId)
//	{
//		return "SELECT ?subject ?content ?to ?cc ?bcc\n"
//				+ "WHERE \n"
//				+ "{\n"
//				+ "      <" + messageId + "> <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.DRAFT_MESSAGE + ">.\n"
//				+ "      <" + accountId + "> <" + Vocabulary.CONTAINS + "> <" + messageId + ">.\n"
//				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_SUBJECT + "> ?subject }\n"
//				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_CONTENT + "> ?content } \n"
//				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.TO + "> ?to } \n"
//				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_CC + "> ?cc } \n"
//				+ "      OPTIONAL { <" + messageId + "> <" + Vocabulary.HAS_BCC + "> ?bcc } \n"
//				+ "}";
//	}
//
//	static String outboxMessageIds(String accountId)
//	{
//		return "SELECT ?messageId\n"
//				+ "WHERE \n"
//				+ "{\n"
//				+ "      ?messageId <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.DRAFT_MESSAGE + ">.\n"
//				+ "      <" + accountId + "> <" + Vocabulary.CONTAINS + "> ?messageId.\n"
//				+ "}";
//	}
}