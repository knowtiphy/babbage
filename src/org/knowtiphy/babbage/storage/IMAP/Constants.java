package org.knowtiphy.babbage.storage.IMAP;

import java.util.regex.Pattern;

public interface Constants
{
	Pattern INBOX_FOLDER_PATTERN = Pattern.compile("inbox", Pattern.CASE_INSENSITIVE);
	Pattern TRASH_FOLDER_PATTERN = Pattern.compile("trash", Pattern.CASE_INSENSITIVE);
	Pattern JUNK_FOLDER_PATTERN = Pattern.compile("spam", Pattern.CASE_INSENSITIVE);
	Pattern SENT_FOLDER_PATTERN = Pattern.compile("sent items", Pattern.CASE_INSENSITIVE);
	Pattern DRAFTS_FOLDER_PATTERN = Pattern.compile("drafts", Pattern.CASE_INSENSITIVE);

	Pattern MSG_JUNK_PATTERN = java.util.regex.Pattern.compile("\\$?Junk", java.util.regex.Pattern.CASE_INSENSITIVE);
	//Pattern MSG_NOT_JUNK_PATTERN = Pattern.compile("(\\$NotJunk)", Pattern.CASE_INSENSITIVE);

	Runnable POISON_PILL = () -> {
	};

	long FREQUENCY = 60000L;
	String JUNK_FLAG = "Junk";
	int NUM_ATTEMPTS = 5;
}
