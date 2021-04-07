package org.knowtiphy.babbage.storage.IMAP;

import org.apache.jena.rdf.model.Model;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.JenaUtils;

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
import java.util.LinkedList;

public class CreateMessage
{
	private static MimeMessage createMessage(IMAPAdapter adapter) throws MessagingException
	{
		Session session = Session.getInstance(adapter.props, new Authenticator()
		{
			@Override
			protected PasswordAuthentication getPasswordAuthentication()
			{
				return new PasswordAuthentication(adapter.emailAddress, adapter.password);
			}
		});

		MimeMessage message = new MimeMessage(session);
		MimeMultipart multipart = new MimeMultipart();
		MimeBodyPart body = new MimeBodyPart();
		multipart.addBodyPart(body);
		message.setContent(multipart);

		return message;
	}

	public static Message createMessage(IMAPAdapter adapter, String oid, Model operation) throws MessagingException, IOException
	{
		var mid = JenaUtils.getOR(operation, oid, Vocabulary.HAS_MESSAGE).toString();

		var recipients = JenaUtils.apply(operation, mid, Vocabulary.TO, to -> to.asLiteral().getString(), new LinkedList<>());
		var ccs = JenaUtils.apply(operation, mid, Vocabulary.HAS_CC, to -> to.asLiteral().getString(), new LinkedList<>());
		var subject = JenaUtils.getS(operation, mid, Vocabulary.HAS_SUBJECT);
		var content = JenaUtils.getS(operation, mid, Vocabulary.HAS_CONTENT);
		var mimeType = JenaUtils.getS(operation, mid, Vocabulary.HAS_MIME_TYPE);

		System.out.println(recipients);
		System.out.println(ccs);
		System.out.println(subject);
		System.out.println(content);
		System.out.println(mimeType);

		var message = createMessage(adapter);
		message.setFrom(new InternetAddress(adapter.emailAddress));
		message.setReplyTo(new Address[]{new InternetAddress(adapter.emailAddress)});

		//  TODO -- get rid of the toList stuff
		for (var to : recipients)
		{
			try
			{
				message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
			}
			catch (AddressException ex)
			{
				//  ignore
			}
		}

		for (var cc : ccs)
		{
			try
			{
				message.addRecipient(Message.RecipientType.CC, new InternetAddress(cc));
			}
			catch (AddressException ex)
			{
				//  ignore
			}
		}

		if (subject != null)
		{
			message.setSubject(subject);
		}

		//  have to have non null content
		((Multipart) message.getContent()).getBodyPart(0).setContent(content == null ? "" : content, mimeType);

		//	setup attachments
//		for (Path path : model.getAttachments())
//		{
//			MimeBodyPart messageBodyPart = new MimeBodyPart();
//			DataSource source = new FileDataSource(path.toFile());
//			messageBodyPart.setDataHandler(new DataHandler(source));
//			messageBodyPart.setFileName(FileUtils.baseName(path.toFile()));
//			((Multipart) message.getContent()).addBodyPart(messageBodyPart);
//		}

		return message;
	}
}

//	private static ParameterizedSparqlString GET_MESSAGE_INFO = new ParameterizedSparqlString(
//			"SELECT ?subject ?content ?to ?cc ?bcc\n"
//					+ "WHERE \n"
//					+ "{\n"
//					+ "      ?aid <" + Vocabulary.CONTAINS + "> ?mid.\n"
//					+ "      OPTIONAL { ?mid <" + Vocabulary.HAS_SUBJECT + "> ?subject }\n"
//					+ "      OPTIONAL { ?mid <" + Vocabulary.HAS_CONTENT + "> ?content } \n"
//					+ "      OPTIONAL { ?mid <" + Vocabulary.TO + "> ?to } \n"
//					+ "      OPTIONAL { ?mid <" + Vocabulary.HAS_CC + "> ?cc } \n"
//					+ "      OPTIONAL { ?mid <" + Vocabulary.HAS_BCC + "> ?bcc } \n"
//					+ "}");
//
//	static List<Address> toList(String raw) throws AddressException
//	{
//		if (raw == null)
//		{
//			return new LinkedList<>();
//		}
//
//		String trim = raw.trim();
//		if (trim.isEmpty())
//		{
//			return new LinkedList<>();
//		}
//
//		String[] tos = trim.split(",");
//		List<Address> result = new ArrayList<>(10);
//		for (String to : tos)
//		{
//			result.add(new InternetAddress(to.trim()));
//		}
//
//		return result;
//	}
