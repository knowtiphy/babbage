package org.knowtiphy.babbage.storage;

import java.nio.file.Path;
import java.util.List;

/**
 * @author graham
 */
public class MessageModel
{
	private final String accountId;
	private final String copyToId;
	private final String subject;
	private final String to;
	private final String cc;
	private final String content;
	private final String mimeType;
	private final List<Path> attachments;


	public MessageModel(String accountId, String subject, String to, String cc, String content,
						String mimeType, List<Path> attachments, String copyToId)
	{
		this.accountId = accountId;
		this.copyToId = copyToId;
		this.subject = subject;
		this.to = to;
		this.cc = cc;
		this.content = content;
		this.mimeType = mimeType;
		this.attachments = attachments;
	}

	public String getAccountId()
	{
		return accountId;
	}

	public String getCopyToId()
	{
		return copyToId;
	}

	public String getSubject()
	{
		return subject;
	}

	public String getTo()
	{
		return to;
	}

	public String getCc()
	{
		return cc;
	}

	public String getContent()
	{
		return content;
	}

	public String getMimeType()
	{
		return mimeType;
	}

	public List<Path> getAttachments()
	{
		return attachments;
	}
}
