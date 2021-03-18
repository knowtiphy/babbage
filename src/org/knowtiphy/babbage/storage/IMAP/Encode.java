package org.knowtiphy.babbage.storage.IMAP;

import org.knowtiphy.babbage.storage.Vocabulary;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Store;
import javax.mail.UIDFolder;
import java.util.Collection;
import java.util.LinkedList;

//	methods to encode javax mail objects as URIs and vica versa

public class Encode
{
	static String encode(Folder folder) throws MessagingException
	{
//		System.out.println("ENCODING");
//		System.out.println(U(folder).getUIDValidity());
//		System.out.println(folder.getURLName());
		//	this may not be unique across accounts ...
//		System.out.println("ENCODING " + folder.getName());
//		System.out.println("ENCODING " + folder.getURLName());
//		System.out.println("ENCODING " + folder.getURLName().toString());
		return folder.getURLName().toString();//Vocabulary.E(Vocabulary.IMAP_FOLDER, emailAddress, U(folder).getUIDValidity());
	}

	static Folder decodeFolder(Store store, String id) throws MessagingException
	{
		int l = id.lastIndexOf("/");
		String fid = id.substring(l + 1);
		return store.getFolder(fid);
	}

	static String encode(Message message) throws MessagingException
	{
		UIDFolder folder = U(message.getFolder());
		return Vocabulary.E(encode((Folder) folder), folder.getUID(message));
//	return Vocabulary.E(Vocabulary.IMAP_MESSAGE, emailAddress, folder.getUIDValidity(), folder.getUID(message));
	}

	static Message decode(Folder folder, String id) throws MessagingException
	{
		int l = id.lastIndexOf("/");
		long mid = Long.parseLong(id.substring(l + 1));
		return U(folder).getMessageByUID(mid);
	}

	static String encode(Message message, String cidName) throws MessagingException
	{
		UIDFolder folder = U(message.getFolder());
		return Vocabulary.E(Vocabulary.IMAP_MESSAGE_CID_PART,
				message.getFolder().getURLName().getHost(), folder.getUIDValidity(), folder.getUID(message), cidName);
	}

	static Message[] U(Folder folder, Collection<String> messageIds)
	{
		//Message[] messages = new Message[messageIds.size()];
		//int i = 0;
		var messages = new LinkedList<Message>();
		for (String message : messageIds)
		{
//			System.out.println("MESSAGE");
//			System.out.println(message);
//			System.out.println(decode(folder, message));
			//	assert m_PerFolderMessage.get(folder).get(message) != null;
			try
			{
				messages.add(Encode.decode(folder, message));//getMm_PerFolderMessage.get(folder).get(message);
			}
			catch (MessagingException e)
			{
				//	the message has been deleted
				e.printStackTrace();
			}
		}

		//noinspection ToArrayCallWithZeroLengthArrayArgument
		return messages.toArray(new Message[messages.size()]);
	}

	static UIDFolder U(Folder folder)
	{
		return (UIDFolder) folder;
	}

}
