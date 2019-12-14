package org.knowtiphy.babbage.storage.IMAP;

import java.util.regex.Pattern;

public interface Constants
{
	//	standard attribute names for IMAP special folders defined in RFC 6154
	Pattern ARCHIVE_PATTERN = Pattern.compile("\\\\Archive");
	Pattern DRAFTS_PATTERN = Pattern.compile("\\\\Drafts");
	Pattern JUNK_ATTRIBUTE = Pattern.compile("\\\\Junk");
	Pattern SENT_ATTRIBUTE = Pattern.compile("\\\\Sent");
	Pattern TRASH_ATTRIBUTE = Pattern.compile("\\\\Trash");

	//	possible names for IMAP special folders -- only use these if we can't find special folders by their attributes
	Pattern[] JUNK_PATTERNS = { Pattern.compile("Spam", Pattern.CASE_INSENSITIVE), Pattern.compile("Junk", Pattern.CASE_INSENSITIVE) };
	Pattern[] SENT_PATTERNS = { Pattern.compile("Sent", Pattern.CASE_INSENSITIVE), Pattern.compile("Sent Items", Pattern.CASE_INSENSITIVE) };
	Pattern[] TRASH_PATTERNS = { Pattern.compile("Trash", Pattern.CASE_INSENSITIVE), Pattern.compile("Deleted", Pattern.CASE_INSENSITIVE) };

	Pattern MSG_JUNK_PATTERN = java.util.regex.Pattern.compile("\\$?Junk", java.util.regex.Pattern.CASE_INSENSITIVE);
	//Pattern MSG_NOT_JUNK_PATTERN = Pattern.compile("(\\$NotJunk)", Pattern.CASE_INSENSITIVE);

	long FREQUENCY = 60000L;
	String JUNK_FLAG = "Junk";
	int NUM_ATTEMPTS = 5;
}
