package org.knowtiphy.babbage.storage.IMAP;

import org.knowtiphy.utils.FileUtils;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class CreateMessage
{

	static List<Address> toList(String raw) throws AddressException
	{
		if (raw == null)
		{
			return new LinkedList<>();
		}

		String trim = raw.trim();
		if (trim.isEmpty())
		{
			return new LinkedList<>();
		}

		String[] tos = trim.split(",");
		List<Address> result = new ArrayList<>(10);
		for (String to : tos)
		{
			result.add(new InternetAddress(to.trim()));
		}

		return result;
	}


	private MimeMessage createMessage(String emailAddress, String password) throws MessagingException
	{
		//	we need system properties to pick up command line flags
		Properties outGoing = System.getProperties();
		//  TODO -- need to get this from the database and per account
		outGoing.setProperty("mail.transport.protocol", "smtp");
		outGoing.setProperty("mail.smtp.host", "chimail.midphase.com");
		outGoing.setProperty("mail.smtp.ssl.enable", "true");
		outGoing.setProperty("mail.smtp.port", "465");
		//	do I need this?
		outGoing.setProperty("mail.smtp.auth", "true");
		//	TODO -- can do this another way?
		Properties props = System.getProperties();
		props.putAll(outGoing);
		Session session = Session.getInstance(props, new Authenticator()
		{
			@Override
			protected PasswordAuthentication getPasswordAuthentication()
			{
				return new PasswordAuthentication(emailAddress, password);
			}
		});

		MimeMessage message = new MimeMessage(session);
		MimeMultipart multipart = new MimeMultipart();
		MimeBodyPart body = new MimeBodyPart();
		multipart.addBodyPart(body);
		message.setContent(multipart);
		return message;
	}

	public Message createMessage(String emailAddress, String passWord, MessageModel model) throws MessagingException, IOException
	{
		Message message = createMessage(emailAddress, passWord);

		//			ReadContext context = getReadContext();
		//			context.start();
		//			try
		//			{
		//				ResultSet resultSet = QueryExecutionFactory
		//						.create(DFetch.outboxMessage(accountId, messageId), context.getModel()).execSelect();
		//				QuerySolution s = resultSet.next();
		//				recipients = s.get(Vars.VAR_TO) == null ? null : s.get(Vars.VAR_TO).asLiteral().getString();
		//				ccs = s.get("cc") == null ? null : s.get("cc").asLiteral().getString();
		//				subject = s.get(Vars.VAR_SUBJECT) == null ? null : s.get(Vars.VAR_SUBJECT).asLiteral().getString();
		//				content = s.get(Vars.VAR_CONTENT) == null ? null : s.get(Vars.VAR_CONTENT).asLiteral().getString();
		//			} finally
		//			{
		//				context.end();
		//			}

		message.setFrom(new InternetAddress(emailAddress));
		message.setReplyTo(new Address[]{new InternetAddress(emailAddress)});
		//  TODO -- get rid of the toList stuff
		if (model.getTo() != null)
		{
			for (Address address : CreateMessage.toList(model.getTo()))
			{
				try
				{
					message.addRecipient(Message.RecipientType.TO, address);
				}
				catch (AddressException ex)
				{
					//  ignore
				}
			}
		}

		if (model.getCc() != null)
		{
			for (Address address : CreateMessage.toList(model.getCc()))
			{
				try
				{
					message.addRecipient(Message.RecipientType.CC, address);
				}
				catch (AddressException ex)
				{
					//  ignore
				}
			}
		}

		if (model.getSubject() != null)
		{
			message.setSubject(model.getSubject());
		}
		//  have to have non null content
		((Multipart) message.getContent()).getBodyPart(0)
				.setContent(model.getContent() == null ? "" : model.getContent(), model.getMimeType());

		//	setup attachments
		for (Path path : model.getAttachments())
		{
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			DataSource source = new FileDataSource(path.toFile());
			messageBodyPart.setDataHandler(new DataHandler(source));
			messageBodyPart.setFileName(FileUtils.baseName(path.toFile()));
			((Multipart) message.getContent()).addBodyPart(messageBodyPart);
		}

		return message;
	}
}
