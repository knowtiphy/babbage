package org.knowtiphy.babbage.storage.IMAP;

public class InlineAttachment extends Attachment
{
	final String localName;

	public InlineAttachment(String id, String mimeType, byte[] content, String localName)
	{
		super(id, mimeType, content);
		this.localName = localName;
	}
}