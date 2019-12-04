package org.knowtiphy.babbage.storage.CARDDAV;

import com.github.sardine.DavResource;
import ezvcard.VCard;
import ezvcard.property.Email;
import ezvcard.property.RawProperty;
import ezvcard.property.Telephone;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Delta;
import org.knowtiphy.babbage.storage.Vocabulary;
import org.knowtiphy.utils.ThreeTuple;

import java.util.Collection;
import java.util.function.Function;

public interface DStore
{
	static Resource R(Model model, String name)
	{
		return model.createResource(name);
	}

	static Property P(ModelCon model, String name)
	{
		return model.createProperty(name);
	}

	static <T> Literal L(ModelCon model, T value)
	{
		return model.createTypedLiteral(value);
	}

	static <S, T> void addAttribute(Delta delta, String subject, String predicate, S value, Function<S, T> fn)
	{
		if (value != null)
		{
			delta.addL(subject, predicate, fn.apply(value));
		}
	}

	static void storeAddressBook(Delta delta, String adapterID, String addressBookId, DavResource addressBook)
	{
		delta.addR(addressBookId, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_ADDRESSBOOK)
				.addR(adapterID, Vocabulary.CONTAINS, addressBookId);

		addAttribute(delta, addressBookId, Vocabulary.HAS_NAME, addressBook.getDisplayName(), x -> x);
		addAttribute(delta, addressBookId, Vocabulary.HAS_CTAG, addressBook.getCustomProps().get("getctag"), x -> x);
	}

	static void storeCard(Delta delta, Collection<ThreeTuple<String, DavResource, VCard>> toProcess,
			String addressBookId, String cardId, VCard vCard, DavResource card)
	{

		if (!vCard.getExtendedProperties().isEmpty())
		{
			// Not sure yet if this list can be any bigger, IE have more than 1 KIND
			if (vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-KIND").get(0).getValue().equals("group"))
			{
				delta.addR(cardId, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_GROUP)
						.addR(addressBookId, Vocabulary.HAS_GROUP, cardId);
			}
			else
			{
				// Dunno, maybe something will eventually spit out being a member instead of nothing
			}
		}
		// I think at this point we can assume its a card
		else
		{
			delta.addR(cardId, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_CARD)
					.addR(addressBookId, Vocabulary.CONTAINS, cardId);

			for (Telephone telephone : vCard.getTelephoneNumbers())
			{
				String phoneNumber = telephone.getText();
				String phoneURI = cardId + "/phone/" + phoneNumber;

				delta.addR(cardId, Vocabulary.HAS_PHONE, phoneURI);

				// Potentially has 1+ types, such home and pref, can account for if we want to
				addAttribute(delta, phoneURI, Vocabulary.HAS_NUMBER, phoneNumber, x -> x);
				addAttribute(delta, phoneURI, Vocabulary.HAS_TYPE, telephone.getTypes().get(0).getValue(), x -> x);
			}

			for (Email email : vCard.getEmails())
			{
				String emailAddress = email.getValue();
				String emailURI = cardId + "/email/" + emailAddress;

				delta.addR(emailURI, Vocabulary.HAS_EMAIL, emailURI);

				// Potentially has 1+ types, such home and pref, can account for if we want to
				addAttribute(delta, emailURI, Vocabulary.HAS_ADDRESS, emailAddress, x -> x);
				addAttribute(delta, emailURI, Vocabulary.HAS_TYPE, email.getTypes().get(0).getValue(), x -> x);

			}
		}

		// This can apparently be empty
		addAttribute(delta, cardId, Vocabulary.HAS_FORMATTED_NAME, vCard.getFormattedName().getValue(), x -> x);
		toProcess.add(new ThreeTuple<>(cardId, card, vCard));
	}

	static void storeCardMeta(Delta delta, ThreeTuple<String, DavResource, VCard> toProcess)
	{
		String cardId = toProcess.fst();
		DavResource card = toProcess.snd();
		VCard vCard = toProcess.thrd();

		if (!vCard.getExtendedProperties().isEmpty())
		{
			// Not sure yet if this list can be any bigger, IE have more than 1 KIND
			if (vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-KIND").get(0).getValue().equals("group"))
			{
				for (RawProperty member : vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-MEMBER"))
				{
					addAttribute(delta, cardId, Vocabulary.HAS_MEMBER_UID, member.getValue(), x -> x);
				}
			}

		}
		else
		{
			addAttribute(delta, cardId, Vocabulary.HAS_UID, vCard.getUid(), x -> x);
		}

		addAttribute(delta, cardId, Vocabulary.HAS_ETAG, card.getEtag(), x -> x);

	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreRes(Model messageDB, Delta delta, String containerName, String resName)
	{
		// System.err.println("DELETING M(" + messageName + ") IN F(" + folderName + " )");
		Resource res = R(messageDB, resName);
		//  TODO -- delete everything reachable from messageName
		StmtIterator it = messageDB.listStatements(res, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(messageDB.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(messageDB.listStatements(res, null, (RDFNode) null));
		delta.delete(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.CONTAINS), res));
	}
}
