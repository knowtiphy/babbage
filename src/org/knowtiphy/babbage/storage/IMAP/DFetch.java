package org.knowtiphy.babbage.storage.IMAP;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.knowtiphy.utils.JenaUtils.P;
import static org.knowtiphy.utils.JenaUtils.R;

/**
 * @author graham
 */
public interface DFetch
{
	ParameterizedSparqlString GET_MESSAGE_UIDS = new ParameterizedSparqlString(
			"select ?mid "
					+ "where {"
					+ "      ?mid <" + RDF.type + "> <" + Vocabulary.IMAP_MESSAGE + ">.\n"
					+ "      ?fid <" + Vocabulary.CONTAINS + "> ?mid.\n"
					+ "      }");

	ParameterizedSparqlString GET_MESSAGE_UIDS_WITH_HEADERS = new ParameterizedSparqlString(
			"select ?mid\n"
					+ "where \n"
					+ "{\n"
					+ "      ?mid <" + RDF.type + "> <" + Vocabulary.IMAP_MESSAGE + ">.\n"
					+ "      ?fid <" + Vocabulary.CONTAINS + "> ?mid.\n"
					+ "      ?aid <" + Vocabulary.CONTAINS + "> ?fid.\n"
					//  if a message has an IS_READ then we have headers for it
					+ "      ?mid <" + Vocabulary.IS_READ + "> ?isRead.\n"
					+ "}");

	ParameterizedSparqlString GET_FOLDER_IDS = new ParameterizedSparqlString(
			"select ?fid "
					+ "where {"
					+ "      ?aid <" + Vocabulary.CONTAINS + "> ?fid.\n"
					+ "      ?fid <" + RDF.type + "> <" + Vocabulary.IMAP_FOLDER + ">.\n"
//	+ "      ?type <" + RDFS.subClassOf + "> <"+ Vocabulary.IMAP_FOLDER +">.\n"
					//+ "      filter(?type = <" + Vocabulary.IMAP_FOLDER + ">).\n"
					+ "      }");

	static boolean hasFolder(Model model, String folderId)
	{
		return model.listStatements(R(model, folderId),
				P(model, RDF.type.toString()), R(model, Vocabulary.IMAP_FOLDER)).hasNext();
	}

	//  get the ids of messages stored in the database that match the query
	private static Set<String> getIDs(Dataset dbase, String query, String proj)
	{
		Set<String> stored = new HashSet<>(1000);
		try (QueryExecution qexec = QueryExecutionFactory.create(query, dbase.getDefaultModel()))
		{
			ResultSet results = qexec.execSelect();
			results.forEachRemaining(soln -> stored.add(soln.get(proj).asResource().toString()));
		}

		return stored;
	}

	static Set<String> getStoredMessageIDs(Dataset cache, String fid)
	{
		GET_MESSAGE_UIDS.setIri("fid", fid);
		return getIDs(cache, GET_MESSAGE_UIDS.toString(), "mid");
	}

	static Set<String> getStoredMessagesWithHeadersIDs(Dataset cache, String aid, String fid)
	{
		GET_MESSAGE_UIDS_WITH_HEADERS.setIri("aid", aid);
		GET_MESSAGE_UIDS_WITH_HEADERS.setIri("fid", fid);
		return getIDs(cache, GET_MESSAGE_UIDS_WITH_HEADERS.toString(), "mid");
	}

	static Collection<String> getFolderIDs(Dataset cache, String aid)
	{
		GET_FOLDER_IDS.setIri(aid, aid);
		return getIDs(cache, GET_FOLDER_IDS.toString(), "fid");
	}
}

//
//	static String outboxMessage(String accountId, String messageId)
//	{
//		return "SELECT ?subject ?content ?to ?cc ?bcc\n"
//				+ "WHERE \n"
//				+ "{\n"
//				+ "      <" + messageId + "> <" + RDF.type.toString() + "> <" + Vocabulary.DRAFT_MESSAGE + ">.\n"
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
//				+ "      ?messageId <" + RDF.type.toString() + "> <" + Vocabulary.DRAFT_MESSAGE + ">.\n"
//				+ "      <" + accountId + "> <" + Vocabulary.CONTAINS + "> ?messageId.\n"
//				+ "}";
//	}}
//
//	static String skeleton(String id)
//	{
//		return String.format("CONSTRUCT\n"
//						+ "{\n"
//						+ "?%s <%s> <%s>.\n"
//						+ "<%s> <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s\n"
//						+ "}\n"
//						+ "WHERE\n"
//						+ "{\n"
//						+ " ?%s <%s> <%s>.\n"
//						+ "<%s> <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
////						+ "?%s <%s> ?%s.\n"
//						+ "}",
//				// START OF CONSTRUCT
//				Vars.VAR_FOLDER_ID, RDF.type.toString(), Vocabulary.IMAP_FOLDER,
//				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
//				Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
//				Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
//				Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_ARCHIVE_FOLDER, Vars.VAR_IS_ARCHIVE_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_DRAFTS_FOLDER, Vars.VAR_IS_DRAFTS_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_INBOX, Vars.VAR_IS_INBOX_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_JUNK_FOLDER, Vars.VAR_IS_JUNK_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_SENT_FOLDER, Vars.VAR_IS_SENT_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_TRASH_FOLDER, Vars.VAR_IS_TRASH_FOLDER,
//				// START OF WHERE
//				Vars.VAR_FOLDER_ID, RDF.type.toString(), Vocabulary.IMAP_FOLDER,
//				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
//				Vars.VAR_FOLDER_ID, Vocabulary.HAS_NAME, Vars.VAR_FOLDER_NAME,
//				Vars.VAR_FOLDER_ID, Vocabulary.HAS_MESSAGE_COUNT, Vars.VAR_MESSAGE_COUNT,
//				Vars.VAR_FOLDER_ID, Vocabulary.HAS_UNREAD_MESSAGE_COUNT, Vars.VAR_UNREAD_MESSAGE_COUNT
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_ARCHIVE_FOLDER, Vars.VAR_IS_ARCHIVE_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_DRAFTS_FOLDER, Vars.VAR_IS_DRAFTS_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_INBOX, Vars.VAR_IS_INBOX_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_JUNK_FOLDER, Vars.VAR_IS_JUNK_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_SENT_FOLDER, Vars.VAR_IS_SENT_FOLDER,
////				Vars.VAR_FOLDER_ID, Vocabulary.IS_TRASH_FOLDER, Vars.VAR_IS_TRASH_FOLDER
//		);
//	}
//
//	static String initialState(String id)
//	{
//		return String.format("CONSTRUCT\n" +
//						"{\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> <%s>.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s.\n"
//						+ "?%s <%s> ?%s }\n"
//						+ "WHERE\n"
//						+ "{\n" + "      "
//						+ "?%s <%s> ?%s.\n" + "      "
//						+ "<%s>  <%s> ?%s.\n" + "      "
//						+ "?%s  <%s> <%s>.\n"
//						+ "?%s  <%s> ?%s.\n" + "      "
//						+ "?%s  <%s> ?%s.\n" + "      "
//						+ "?%s  <%s> ?%s.\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "OPTIONAL { ?%s  <%s> ?%s }\n"
//						+ "}",
//				// START OF CONSTRUCT
//				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
//				Vars.VAR_MESSAGE_ID, RDF.type.toString(), Vocabulary.IMAP_MESSAGE,
//				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
//				Vars.VAR_MESSAGE_ID, Vocabulary.IS_READ, Vars.VAR_IS_READ,
//				Vars.VAR_MESSAGE_ID, Vocabulary.IS_JUNK, Vars.VAR_IS_JUNK,
//				Vars.VAR_MESSAGE_ID, Vocabulary.IS_ANSWERED, Vars.VAR_IS_ANSWERED,
//				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_SUBJECT, Vars.VAR_SUBJECT,
//				Vars.VAR_MESSAGE_ID, Vocabulary.RECEIVED_ON, Vars.VAR_RECEIVED_ON,
//				Vars.VAR_MESSAGE_ID, Vocabulary.SENT_ON, Vars.VAR_SENT_ON,
//				Vars.VAR_MESSAGE_ID, Vocabulary.TO, Vars.VAR_TO,
//				Vars.VAR_MESSAGE_ID, Vocabulary.FROM, Vars.VAR_FROM,
//				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_CC, Vars.VAR_CC,
//				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_BCC, Vars.VAR_BCC,
//				// START OF WHERE
//				Vars.VAR_FOLDER_ID, Vocabulary.CONTAINS, Vars.VAR_MESSAGE_ID,
//				id, Vocabulary.CONTAINS, Vars.VAR_FOLDER_ID,
//				Vars.VAR_MESSAGE_ID, RDF.type.toString(), Vocabulary.IMAP_MESSAGE,
//				Vars.VAR_MESSAGE_ID, Vocabulary.IS_READ, Vars.VAR_IS_READ,
//				Vars.VAR_MESSAGE_ID, Vocabulary.IS_JUNK, Vars.VAR_IS_JUNK,
//				Vars.VAR_MESSAGE_ID, Vocabulary.IS_ANSWERED, Vars.VAR_IS_ANSWERED,
//				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_SUBJECT, Vars.VAR_SUBJECT,
//				Vars.VAR_MESSAGE_ID, Vocabulary.RECEIVED_ON, Vars.VAR_RECEIVED_ON,
//				Vars.VAR_MESSAGE_ID, Vocabulary.SENT_ON, Vars.VAR_SENT_ON,
//				Vars.VAR_MESSAGE_ID, Vocabulary.TO, Vars.VAR_TO,
//				Vars.VAR_MESSAGE_ID, Vocabulary.FROM, Vars.VAR_FROM,
//				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_CC, Vars.VAR_CC,
//				Vars.VAR_MESSAGE_ID, Vocabulary.HAS_BCC, Vars.VAR_BCC);
//	}