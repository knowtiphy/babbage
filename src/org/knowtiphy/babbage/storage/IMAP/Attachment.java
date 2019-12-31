package org.knowtiphy.babbage.storage.IMAP;

public class Attachment
{
	final String id;
	final String mimeType;
	final byte[] content;

	public Attachment(String id, String mimeType, byte[] content)
	{
		this.id = id;
		this.mimeType = mimeType;
		this.content = content;
	}
}