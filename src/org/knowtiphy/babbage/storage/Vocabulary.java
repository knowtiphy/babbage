package org.knowtiphy.babbage.storage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author graham
 */
public class Vocabulary
{
	public final static String BASE = "http://www.knowtiphy.org/";
	public final static String TBASE = BASE + "Terminology/";
	public final static String NBASE = BASE + "name/";

	//	account vocabulary
	public final static String ACCOUNT = TBASE + "Account";

	public final static String OUTBOX = TBASE + "OutBox";
	public final static String OUTBOX_MESSAGE = TBASE + "IMAPOutBoxMessage";

	public final static String HAS_SERVER_NAME = TBASE + "hasServerName";
	public final static String HAS_EMAIL_ADDRESS = TBASE + "hasEmailAddress";
	public final static String HAS_PASSWORD = TBASE + "hasPassword";
	public final static String HAS_LAST_SEARCH_DATE = TBASE + "hasLastSearchDate";
	public final static String HAS_SERVER_HEADER = TBASE + "hasServerHeader";
	public final static String HAS_NICK_NAME = TBASE + "hasNickName";

	public final static String HAS_TRUSTED_CONTENT_PROVIDER = TBASE + "hasTrustedContentProvider";
	public final static String HAS_TRUSTED_SENDER = TBASE + "hasTrustedSender";
	//
	public final static String DRAFT_MESSAGE = TBASE + "DraftMessage";

	//	IMAP vocabulary
	public final static String HAS_IMAP_SERVER = TBASE + "hasIMAPServer";
	public final static String HAS_SMTP_SERVER = TBASE + "hasSMTPServer";
	public final static String IMAP_ACCOUNT = TBASE + "IMAPAccount";
	public final static String IMAP_FOLDER = TBASE + "IMAPFolder";
	public final static String INBOX_FOLDER = TBASE + "InboxFolder";
	public final static String TRASH_FOLDER = TBASE + "TrashFolder";
	public final static String JUNK_FOLDER = TBASE + "JunkFolder";
	public final static String ARCHIVE_FOLDER = TBASE + "ArchiveFolder";
	public final static String SENT_FOLDER = TBASE + "SentFolder";
	public final static String DRAFTS_FOLDER = TBASE + "DraftsFolder";
	public final static String IMAP_MESSAGE = TBASE + "IMAPMessage";
	public final static String IMAP_MESSAGE_PART = TBASE + "IMAPMessagePart";
	public final static String IMAP_MESSAGE_CID_PART = TBASE + "IMAPMessageCIDPart";
	public final static String IMAP_MESSAGE_ATTACHMENT = TBASE + "Attachment";

	public final static String HAS_SPECIAL = TBASE + "hasSpecial";
	public final static String HAS_NAME = TBASE + "hasName";
	public final static String HAS_UID_VALIDITY = TBASE + "hasUIDValidity";
	public final static String HAS_MESSAGE_COUNT = TBASE + "hasMessageCount";
	public final static String HAS_UNREAD_MESSAGE_COUNT = TBASE + "hasUnreadMessageCount";
//    public final static String IS_JUNK_FOLDER = TBASE + "isJunkFolder";
//    public final static String IS_INBOX = TBASE + "isInBox";
//    public final static String IS_ARCHIVE_FOLDER = TBASE + "isArchiveFolder";
//    public final static String IS_DRAFTS_FOLDER = TBASE + "isDraftsFolder";
//    public final static String IS_SENT_FOLDER = TBASE + "isSentFolder";
//    public final static String IS_TRASH_FOLDER = TBASE + "isTrashFolder";

	public final static String CONTAINS = TBASE + "contains";

	public final static String IS_READ = TBASE + "isRead";
	public final static String IS_JUNK = TBASE + "isJunk";
	public final static String IS_ANSWERED = TBASE + "isAnswered";

	public final static String HAS_SUBJECT = TBASE + "hasSubject";
	public final static String TO = TBASE + "to";
	public final static String FROM = TBASE + "from";
	public final static String HAS_CC = TBASE + "hasCC";
	public final static String HAS_BCC = TBASE + "hasBCC";
	public final static String RECEIVED_ON = TBASE + "receivedOn";
	public final static String SENT_ON = TBASE + "sentOn";

	public final static String HAS_ATTACHMENT = TBASE + "hasAttachment";
	public final static String HAS_CID_PART = TBASE + "hasCIDPart";
	public final static String HAS_CONTENT = TBASE + "hasContent";
	public final static String HAS_FILE_NAME = TBASE + "hasFileName";
	public final static String HAS_MIME_TYPE = TBASE + "hasMimeType";
	public final static String HAS_LOCAL_CID = TBASE + "hasLocalCID";

	// Vocabulary for CalDav
	public final static String CALDAV_ACCOUNT = TBASE + "CALDAVAccount";
	public final static String CALDAV_CALENDAR = TBASE + "CALDAVCalendar";
	public final static String CALDAV_EVENT = TBASE + "CALDAVEvent";
	public final static String HAS_CTAG = TBASE + "hasCTag";
	public final static String HAS_EVENT = TBASE + "hasEvent";

	public final static String HAS_DATE_START = TBASE + "startsAt";
	public final static String HAS_DATE_END = TBASE + "endsAt";
	public final static String HAS_SUMMARY = TBASE + "hasSummary";
	public final static String HAS_LOCATION = TBASE + "hasLocation";
	public final static String HAS_DESCRIPTION = TBASE + "hasDescription";
	public final static String HAS_PRIORITY = TBASE + "hasPriority";
	public final static String HAS_ETAG = TBASE + "hasETag";

	// Vocabulary for CardDav
	public final static String CARDDAV_ACCOUNT = TBASE + "CARDDAVAccount";
	public final static String CARDDAV_ADDRESSBOOK = TBASE + "CARDDAVAddressBook";
	public final static String HAS_GROUP = TBASE + "hasGroup";
	public final static String CARDDAV_GROUP = TBASE + "CARDDAVGroup";
	public final static String HAS_CARD = TBASE + "hasCard";
	public final static String HAS_MEMBER_UID = TBASE + "hasMemberUID";
	public final static String CARDDAV_CARD = TBASE + "CARDDAVCard";

	public final static String HAS_UID = TBASE + "hasUID";
	public final static String HAS_PHONE = TBASE + "hasPhone";
	public final static String HAS_NUMBER = "hasNumber";
	public final static String HAS_TYPE = TBASE + "hasType";
	public final static String HAS_EMAIL = TBASE + "hasEmail";
	public final static String HAS_ADDRESS = TBASE + "hasAddress";

	//  event vocabulary

	public final static String EVENT = TBASE + "EVENT";
	public final static String ACCOUNT_SYNCED = TBASE + "AccountSynced";
	public final static String FOLDER_SYNCED = TBASE + "FolderSynced";
	public final static String MESSAGE_FLAGS_CHANGED = TBASE + "MessageFlagsChanged";
	public final static String MESSAGE_ARRIVED = TBASE + "MessageArrived";
	public final static String MESSAGE_DELETED = TBASE + "MessageDeleted";
	public final static String HAS_ACCOUNT = TBASE + "hasAcount";
	public final static String HAS_CALENDAR = TBASE + "hasCalendar";
	public final static String HAS_FOLDER = TBASE + "hasFolder";
	public final static String HAS_MESSAGE = TBASE + "hasMessage";
	public final static String HAS_FLAG = TBASE + "hasFlag";

	//	operations vocabulary

	public final static String OPERATION = TBASE + "Operation";
	public final static String SYNC = TBASE + "Sync";
	public final static String SYNC_AHEAD = TBASE + "SyncAhead";
	public final static String MARK_READ = TBASE + "MarkReadOperation";
	public final static String MARK_JUNK = TBASE + "MarkJunkOperation";
	public final static String MARK_ANSWERED = TBASE + "MarkAnsweredOperation";
	public final static String DELETE_MESSAGE = TBASE + "DeleteMessageOperation";
	public final static String SEND_MESSAGE = TBASE + "SendMessageOperation";
	public final static String TRUST_SENDER = TBASE + "TrustSender";
	public final static String TRUST_PROVIDER = TBASE + "TrustProvider";
	public final static String HAS_RESOURCE = TBASE + "hasResource";

	public final static String ADD_CALDAV_EVENT = TBASE + "addCalDavEvent";

	//	sub-classing structure

	public final static Map<String, String> eventSubClasses = new HashMap<>();

	static
	{
		eventSubClasses.put(ACCOUNT_SYNCED, EVENT);
		eventSubClasses.put(FOLDER_SYNCED, EVENT);
		eventSubClasses.put(MESSAGE_FLAGS_CHANGED, EVENT);
		eventSubClasses.put(MESSAGE_ARRIVED, EVENT);
		eventSubClasses.put(MESSAGE_DELETED, EVENT);
	}

	public final static Map<String, String> accountSubClasses = new HashMap<>();

	static
	{
		accountSubClasses.put(IMAP_ACCOUNT, ACCOUNT);
		accountSubClasses.put(CALDAV_ACCOUNT, ACCOUNT);
		accountSubClasses.put(CARDDAV_ACCOUNT, ACCOUNT);
	}

	public final static Map<String, String> folderSubClasses = new HashMap<>();

	static
	{
		folderSubClasses.put(INBOX_FOLDER, IMAP_FOLDER);
		folderSubClasses.put(TRASH_FOLDER, IMAP_FOLDER);
		folderSubClasses.put(SENT_FOLDER, IMAP_FOLDER);
		folderSubClasses.put(JUNK_FOLDER, IMAP_FOLDER);
		folderSubClasses.put(ARCHIVE_FOLDER, IMAP_FOLDER);
		folderSubClasses.put(DRAFTS_FOLDER, IMAP_FOLDER);
	}

	public final static Map<String, String> operationsubClasses = new HashMap<>();

	static
	{
		operationsubClasses.put(SYNC, OPERATION);
		operationsubClasses.put(SYNC_AHEAD, OPERATION);
		operationsubClasses.put(DELETE_MESSAGE, OPERATION);
		operationsubClasses.put(MARK_READ, OPERATION);
		operationsubClasses.put(MARK_JUNK, OPERATION);
		operationsubClasses.put(MARK_ANSWERED, OPERATION);
		operationsubClasses.put(SEND_MESSAGE, OPERATION);
		operationsubClasses.put(TRUST_SENDER, OPERATION);
		operationsubClasses.put(TRUST_PROVIDER, OPERATION);
		operationsubClasses.put(ADD_CALDAV_EVENT, OPERATION);
	}

	public final static Map<String, String> allSubClasses = new HashMap<>();

	static
	{
		allSubClasses.putAll(accountSubClasses);
		allSubClasses.putAll(eventSubClasses);
		allSubClasses.putAll(operationsubClasses);
		allSubClasses.putAll(folderSubClasses);
	}

	public static String E(Object... parts)
	{
		var builder = new StringBuilder();
		for (int i = 0; i < parts.length - 1; i++)
		{
			builder.append(parts[i].toString()).append("/");
		}
		builder.append(parts[parts.length - 1].toString());
		return builder.toString();
	}
}