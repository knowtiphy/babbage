package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IMAP.MessageModel;
import org.knowtiphy.babbage.storage.IStorage;
import org.knowtiphy.babbage.storage.exceptions.StorageException;

import java.nio.file.Path;
import java.util.List;

public class SendMessage implements IMessage
{
	private final String accountId;
	private final String copyToId;
	private final String subject;
	private final String to;
	private final String cc;
	private final String content;
	private final String mimeType;
	private final List<Path> attachments;

	public SendMessage(String accountId, String copyToId, String subject, String to, String cc, String content,
					   String mimeType, List<Path> attachments)
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

	@Override
	public void perform(IStorage storage) throws StorageException
	{
		storage.send(new MessageModel(accountId, subject, to, cc, content, mimeType, attachments, copyToId));
	}
}