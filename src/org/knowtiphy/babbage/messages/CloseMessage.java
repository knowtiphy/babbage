package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;

public class CloseMessage implements IMessage
{
	@Override
	public void perform(IStorage storage)
	{
		storage.close();
	}
}
