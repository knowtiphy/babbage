package org.knowtiphy.babbage.messages;

import org.knowtiphy.babbage.storage.IStorage;

@FunctionalInterface
public interface IMessage
{
	// Cast to this interface and call it's method
	void perform(IStorage storage) throws Exception;
}
