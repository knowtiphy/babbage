package org.knowtiphy.babbage.storage.IMAP;

public class RegularAttachment extends Attachment
{
	final String fileName;

	public RegularAttachment(String id, String mimeType, byte[] content, String fileName)
	{
		super(id, mimeType, content);
		this.fileName = fileName;
	}
}