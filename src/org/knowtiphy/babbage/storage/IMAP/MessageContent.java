package org.knowtiphy.babbage.storage.IMAP;

import org.knowtiphy.utils.ThrowingConsumer;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author graham
 */
public class MessageContent
{
	private final Message message;
	private final boolean allowHTML;
	private final Map<String, Part> cidMap = new HashMap<>(30);
	private final Collection<Part> attachments = new LinkedList<>();
	private Part content;

	public MessageContent(Message message, boolean allowHTML)
	{
		assert message != null;
		this.message = message;
		this.allowHTML = allowHTML;
	}

	public Part getContent()
	{
		return content;
	}

	public Map<String, Part> getCidMap()
	{
		return cidMap;
	}

	public Collection<Part> getAttachments()
	{
		return attachments;
	}

	private static boolean isAttachment(Part part) throws MessagingException
	{
		String disposition = part.getDisposition();
		return Part.ATTACHMENT.equalsIgnoreCase(disposition);
	}

	private static String cid(String cid)
	{
		int pos1 = cid.indexOf('<');
		int pos2 = cid.indexOf('>');
		if (pos1 == -1 && pos2 == -1)
		{
			return cid;
		}
		try
		{
			return cid.substring(pos1 + 1, pos2);
		} catch (StringIndexOutOfBoundsException ex)
		{
			return null;
		}
	}

	private static boolean hasValidCID(Part part)
	{
		//  TODO -- is this correct?
		//noinspection ProhibitedExceptionCaught
		try
		{
			//noinspection ResultOfObjectAllocationIgnored
			new URL("cid:" + cid(part.getHeader("Content-Id")[0]));
			return true;
		} catch (MessagingException | MalformedURLException | NullPointerException ex)
		{
			return false;
		}
	}

	/**
	 * Return the primary text content of the message.
	 */
	private Part getText(Part part) throws MessagingException, IOException
	{
		if (isAttachment(part))
		{
			return null;
		}

		if (part.isMimeType(Mime.TEXT))
		{
			return part;
		}

		if (part.isMimeType(Mime.MULTIPART_ALTERNATIVE))
		{
			//System.err.println("MULTI/ALT");
			// prefer html text over plain text
			Multipart mp = (Multipart) part.getContent();
			Part text = null;
			for (int i = 0; i < mp.getCount(); i++)
			{
				Part bp = mp.getBodyPart(i);
				if (!isAttachment(bp))
				{
					if (bp.isMimeType(Mime.PLAIN))
					{
						if (!allowHTML)
						{
							return getText(bp);
						}
						text = getText(bp);
					}
					else if (bp.isMimeType(Mime.HTML) && allowHTML)
					{
						//System.err.println("HTML P = " + bp);
						return getText(bp);
					}
					else
					{
						return getText(bp);
					}
				}
			}

			return text;
		}
		else if (part.isMimeType(Mime.MULTIPART))
		{
			//System.err.println("MULTI/REGULAR");
			Multipart mp = (Multipart) part.getContent();
			Part text = null;
			for (int i = 0; i < mp.getCount(); i++)
			{
				Part p = mp.getBodyPart(i);
				if (!isAttachment(p))
				{
					Part bp = getText(p);
					if (bp != null)
					{
						if (bp.isMimeType(Mime.PLAIN))
						{
							assert !(bp instanceof Multipart);
							if (!allowHTML)
							{
								return bp;
							}
							text = bp;
						}
						else if (bp.isMimeType(Mime.HTML) && allowHTML)
						{
							assert !(bp instanceof Multipart);
							return bp;
						}
					}
				}
			}

			return text;
		}

		return null;
	}

	private static void walk(Part part, ThrowingConsumer<? super Part, ? extends MessagingException> process) throws MessagingException, IOException
	{
		process.apply(part);

		if (part.isMimeType(Mime.MULTIPART))
		{
			Multipart mp = (Multipart) part.getContent();
			for (int i = 0; i < mp.getCount(); i++)
			{
				Part p = mp.getBodyPart(i);
				walk(p, process);
			}
		}
	}

	private void parseAttachments() throws MessagingException, IOException
	{
		Collection<Part> inline = new LinkedList<>();
		walk(message, part ->
		{
			String disposition = part.getDisposition();
			if (disposition != null)
			{
				if (disposition.equalsIgnoreCase(Part.ATTACHMENT))
				{
					attachments.add(part);
				}
				else if (disposition.equalsIgnoreCase(Part.INLINE))
				{
					if (hasValidCID(part))
					{
						inline.add(part);
					}
					else
					{
						attachments.add(part);
					}
				}
			}
		});

		for (Part part : inline)
		{
			String cidName = cid(part.getHeader("Content-Id")[0]);
			cidMap.put("cid:" + cidName, part);
		}
	}

	protected void process() throws MessagingException, IOException
	{
		content = getText(message);
		parseAttachments();
	}
}

//	PartState(Multipart part, int current)
//	{
//		this.part = part;
//		this.current = current;
//	}
//
//	@Override
//	public String toString()
//	{
//		try
//		{
//			return "PartState{" + "part=" + part + ", current=" + current + ", " + part.getCount() + '}';
//		} catch (MessagingException ex)
//		{
//			logger.log(Level.SEVERE, null, ex);
//			return "WTF";
//		}
//	}
//}
//	walker based getPart()
//	{
//		ensureContentLoaded();
//		MessageWalker walker = new MessageWalker(message);
//		Part result = null;
//		//	TODO -- more efficient to pass textOnly down into the walker
//		while (walker.hasNext())
//		{
//			Part bp = walker.next();
//			String disposition = bp.getDisposition();
//			//	ignore attachments
//			if (disposition != null && disposition.equals(Part.ATTACHMENT))
//			{
//				continue;
//			}
//			String contentType = bp.getContentType();
//			if (MessageHelper.isHTML(contentType))
//			{
//				if (allowHTML)
//				{
//					return bp;
//				}
//				result = bp;
//			}
//			if (MessageHelper.isText(contentType))
//			{
//				// found text part -- remember it and keep looking for an HTML part
//				result = bp;
//				if (!allowHTML)
//				{
//					break;
//				}
//			}
//		}
//
//		if (result != null)
//		{
//			return result;
//		}
//		else
//		{
//			System.err.println("HUH : can't decode the message -- guessing");
//			return message;
//		}
//	}
//	walker based getAttachments
//	ensureContentLoaded();
//		MessageWalker walker = new MessageWalker(message);
//		List<Part> result = new LinkedList<>();
//		while (walker.hasNext())
//		{
//			Part bp = walker.next();
//			String disposition = bp.getDisposition();
//			if (disposition != null && disposition.equals(Part.ATTACHMENT))
//			{
//				result.add(bp);
//			}
//		}
//
//		return result;
//
//
//public class MessageWalker
//{
//	//	stores multipart org.knowtiphy.pinkpigmail.messages
//	private final Stack<PartState> partStack;
//	//	not really a stack, only ever has one thing on it
//	private final Stack<Part> contentStack;
//
//	public MessageWalker(Message message) throws MessagingException, IOException
//	{
//		contentStack = new Stack<>();
//		partStack = new Stack<>();
//		//	todo -- lazily load them?
//		Object content = message.getContent();
//		if (content instanceof Part || content instanceof Multipart)
//		{
//			down(content);
//		}
//		else
//		{
//			//	the message implements Part
//			contentStack.push(message);
//		}
//	}
//
//	private void down(Object part) throws MessagingException, IOException
//	{
//		if (part instanceof Multipart)
//		{
//			Multipart multi = (Multipart) part;
//			partStack.push(new MessageWalker.PartState(multi, 1));
//			down(multi.getBodyPart(0));
//		}
//		else
//		{
//			//	which idiots send mail like this?
//			assert part instanceof Part;
//			Part p = (Part) part;
//			if (p.getContent() instanceof Multipart)
//			{
//				Multipart multi = (Multipart) p.getContent();
//				partStack.push(new MessageWalker.PartState(multi, 1));
//				down(multi.getBodyPart(0));
//			}
//			else
//			{
//				contentStack.push(p);//new ContentDescriptor(p.getContent(), p.getContentType()));
//			}
//		}
//	}
//		else
//		{
//			assert false;
//			//contentStack.push(new ContentDescriptor(part, contentType));
//		}
//	}
//
//	public boolean hasNext() throws MessagingException, IOException
//	{
//		if (contentStack.isEmpty() && partStack.isEmpty())
//		{
//			return false;
//		}
//		else
//		{
//			while (!partStack.isEmpty() && partStack.peek().part instanceof Multipart)
//			{
//				MessageWalker.PartState state = partStack.peek();
//				if (state.current < state.part.getCount())
//				{
//					//	go down the next one
//					down(state.part.getBodyPart(state.current));
//					state.current++;
//					return true;
//				}
//				else
//				{
//					//	done with this part
//					partStack.pop();
//				}
//			}
//
//			return !contentStack.isEmpty();
//		}
//	}
//
//	public Part next() throws MessagingException
//	{
//		return contentStack.pop();
//	}
//
//	private class PartState
//	{
//		Multipart part;
//		int current;
//
//		PartState(Multipart part, int current)
//		{
//			this.part = part;
//			this.current = current;
//		}
//
//		@Override
//		public String toString()
//		{
//			try
//			{
//				return "PartState{" + "part=" + part + ", current=" + current + ", " + part.getCount() + '}';
//			}
//			catch (MessagingException ex)
//			{
//				Logger.getLogger(MessageWalker.class.getName()).log(Level.SEVERE, null, ex);
//				return "WTF";
//			}
//		}
//	}
//}