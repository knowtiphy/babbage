package org.knowtiphy.babbage.storage;

/**
 *
 * @author graham
 */
public interface Vocabulary
{
    String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    String BASE = "http://www.knowtiphy.org/";
    String TBASE = BASE + "Terminology/";
    String NBASE = BASE + "name/";

    String ACCOUNT = TBASE + "Account";

    String OUTBOX = TBASE + "OutBox";
    String OUTBOX_MESSAGE = TBASE + "IMAPOutBoxMessage";

    String HAS_SERVER_NAME = TBASE + "hasServerName";
    String HAS_EMAIL_ADDRESS = TBASE + "hasEmailAddress";
    String HAS_PASSWORD = TBASE + "hasPassword";
    String HAS_SERVER_HEADER = TBASE + "hasServerHeader";
    String HAS_NICK_NAME = TBASE + "hasNickName";

    String HAS_TRUSTED_CONTENT_PROVIDER = TBASE + "hasTrustedContentProvider";
	String HAS_TRUSTED_SENDER = TBASE + "hasTrustedSender";
    //
    String DRAFT_MESSAGE = TBASE + "DraftMessage";

    String IMAP_ACCOUNT = TBASE + "IMAPAccount";
    String IMAP_FOLDER = TBASE + "IMAPFolder";
    String IMAP_MESSAGE = TBASE + "IMAPMessage";
    String IMAP_MESSAGE_PART = TBASE + "IMAPMessagePart";
    String IMAP_MESSAGE_CID_PART = TBASE + "IMAPMessageCIDPart";
    String IMAP_MESSAGE_ATTACHMENT = TBASE + "Attachment";

    String HAS_NAME = TBASE + "hasName";
    String HAS_UID_VALIDITY = TBASE + "hasUIDValidity";
    String HAS_MESSAGE_COUNT = TBASE + "hasMessageCount";
    String HAS_UNREAD_MESSAGE_COUNT = TBASE + "hasUnreadMessageCount";
    String IS_JUNK_FOLDER = TBASE + "isJunkFolder";
    String IS_INBOX = TBASE + "isInBox";
    String IS_TRASH_FOLDER = TBASE + "isTrashFolder";

    String CONTAINS = TBASE + "contains";

    String IS_READ = TBASE + "isRead";
    String IS_JUNK = TBASE + "isJunk";
    String IS_ANSWERED = TBASE + "isAnswered";

    String HAS_SUBJECT = TBASE + "hasSubject";
    String TO = TBASE + "to";
    String FROM = TBASE + "from";
    String HAS_CC = TBASE + "hasCC";
    String HAS_BCC = TBASE + "hasBCC";
    String RECEIVED_ON = TBASE + "receivedOn";
    String SENT_ON = TBASE + "sentOn";

    String HAS_ATTACHMENT = TBASE + "hasAttachment";
    String HAS_CID_PART = TBASE + "hasCIDPart";
    String HAS_CONTENT = TBASE + "hasContent";
    String HAS_FILE_NAME = TBASE + "hasFileName";
    String HAS_MIME_TYPE = TBASE + "hasMimeType";
    String HAS_LOCAL_CID = TBASE + "hasLocalCID";

    // Vocabulary for CalDav
    String CALDAV_ACCOUNT = TBASE + "CALDAVAccount";
    String CALDAV_CALENDAR = TBASE + "CALDAVCalendar";
    String CALDAV_EVENT = TBASE + "CALDAVEvent";
    String HAS_CTAG = TBASE + "hasCTag";

    String HAS_DATE_START = TBASE + "startsAt";
    String HAS_DATE_END = TBASE + "endsAt";
    String HAS_SUMMARY = TBASE + "hasSummary";
    String HAS_DESCRIPTION = TBASE + "hasDescription";
    String HAS_PRIORITY = TBASE + "hasPriority";
    String HAS_ETAG = TBASE + "hasETag";

    // Vocabulary for CardDav
    String CARDDAV_ACCOUNT = TBASE + "CARDDAVAccount";
    String CARDDAV_ADDRESSBOOK = TBASE + "CARDDAVAddressBook";
    String CARDDAV_GROUP = TBASE + "CARDDAVGroup";
    String HAS_CARD = TBASE + "hasCard";
    String CARDDAV_CARD = TBASE + "CARDDAVCard";

    String HAS_UID = TBASE + "hasUID";
    String HAS_FORMATTED_NAME = TBASE + "formattedName";
    String HAS_PHONE_NUMBER = TBASE + "hasNumber";
    String HAS_PHONE_TYPE = TBASE + "hasPhoneType";
    String HAS_EMAIL = TBASE + "hasEmail";
    String HAS_EMAIL_TYPE = TBASE + "hasEmailType";


    static String E(Object... parts)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++)
        {
            builder.append(parts[i].toString()).append("/");
        }
        builder.append(parts[parts.length - 1].toString());
        return builder.toString();
    }
}
