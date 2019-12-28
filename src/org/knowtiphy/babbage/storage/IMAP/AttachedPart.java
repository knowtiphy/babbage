package org.knowtiphy.babbage.storage.IMAP;

public class AttachedPart
{
	public final String mimeType;
	public final byte[] content;
	//	only for non-inline attachments, null otherwise
	public final String fileName;

	public AttachedPart(String mimeType, byte[] content, String fileName)
	{
		this.mimeType = mimeType;
		this.content = content;
		this.fileName = fileName;
	}

	public AttachedPart(String mimeType, byte[] content)
	{
		this(mimeType, content, null);
	}
}