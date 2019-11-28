package org.knowtiphy.babbage.storage.IMAP;

public interface Pattern
{
	java.util.regex.Pattern INBOX = java.util.regex.Pattern.compile("INBOX");

	java.util.regex.Pattern MSG_JUNK_PATTERN = java.util.regex.Pattern.compile("\\$?Junk", java.util.regex.Pattern.CASE_INSENSITIVE);
	//Pattern MSG_NOT_JUNK_PATTERN = Pattern.compile("(\\$NotJunk)", Pattern.CASE_INSENSITIVE);
}
