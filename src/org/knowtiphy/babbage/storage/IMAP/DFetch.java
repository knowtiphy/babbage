package org.knowtiphy.babbage.storage.IMAP;

import org.knowtiphy.babbage.storage.Vocabulary;

/**
 *
 * @author graham
 */
public interface DFetch
{
    //private final static Pattern MSG_NOT_JUNK_PATTERN = Pattern.compile("(\\$NotJunk)", Pattern.CASE_INSENSITIVE);
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

    static String messageUIDsWithContent(String folderName)
    {
        return "SELECT ?message "
            + "WHERE {"
            + "      ?message <" + Vocabulary.RDF_TYPE + "> <" + Vocabulary.IMAP_MESSAGE + ">.\n"
            + "      ?message <" + Vocabulary.HAS_MIME_TYPE + "> ?mimeType.\n"
            + "      <" + folderName + "> <" + Vocabulary.CONTAINS + "> ?message.\n"
            + "      }";
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