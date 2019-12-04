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

import java.util.ArrayList;
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

	static void storeMemberCards(Delta delta, String groupID, Collection<String> memberID)
	{
		memberID.forEach(id -> addAttribute(delta, groupID, Vocabulary.HAS_CARD, id, x -> x));
	}

	static void storeAddressBook(Delta delta, String adapterID, String addressBookId, DavResource addressBook)
	{
		delta.addR(addressBookId, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_ADDRESSBOOK)
				.addR(adapterID, Vocabulary.CONTAINS, addressBookId);

		addAttribute(delta, addressBookId, Vocabulary.HAS_NAME, addressBook.getDisplayName(), x -> x);

		System.out.println("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB :: " + addressBook.getCustomProps()
				.get("getctag"));

		addAttribute(delta, addressBookId, Vocabulary.HAS_CTAG, addressBook.getCustomProps().get("getctag"), x -> x);
	}

	static void storeResource(Delta delta, Collection<ThreeTuple<String, DavResource, VCard>> toProcess,
			String addressBookId, String cardId, VCard vCard, DavResource card)
	{
		boolean hasEmail = false;
		if (!vCard.getExtendedProperties().isEmpty() && vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-KIND").get(0)
				.getValue().equals("group"))
		{

			delta.addR(cardId, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_GROUP)
					.addR(addressBookId, Vocabulary.HAS_GROUP, cardId);
		}
		// I think at this point we can assume its a card
		else
		{
			delta.addR(cardId, Vocabulary.RDF_TYPE, Vocabulary.CARDDAV_CARD)
					.addR(addressBookId, Vocabulary.CONTAINS, cardId);

			for (Telephone telephone : vCard.getTelephoneNumbers())
			{
				String phoneNumber = telephone.getText().trim();
				String phoneURI = cardId + "/phone/" + phoneNumber;

				delta.addR(cardId, Vocabulary.HAS_PHONE, phoneURI);

				// Potentially has 1+ types, such home and pref, can account for if we want to
				addAttribute(delta, phoneURI, Vocabulary.HAS_NUMBER, phoneNumber, x -> x);
				addAttribute(delta, phoneURI, Vocabulary.HAS_TYPE, telephone.getTypes().get(0).getValue(), x -> x);
			}

			for (Email email : vCard.getEmails())
			{
				hasEmail = true;
				String emailAddress = email.getValue().replaceAll("\\s", "");
				String emailURI = cardId + "/email/" + emailAddress;

				delta.addR(emailURI, Vocabulary.HAS_EMAIL, emailURI);

				// Potentially has 1+ types, such home and pref, can account for if we want to
				addAttribute(delta, emailURI, Vocabulary.HAS_ADDRESS, emailAddress, x -> x);
				addAttribute(delta, emailURI, Vocabulary.HAS_TYPE, email.getTypes().get(0).getValue(), x -> x);

			}
		}

		// This can apparently be empty, so will fill with an email first, and then an tele if none found
		// I just had fun doing this, it's too late.
		// If they don't have an email or a tele, well they suck, can account for that later

		String name = vCard.getFormattedName().getValue();

		addAttribute(delta, cardId, Vocabulary.HAS_NAME, !name.equals("") && !name.equals(" ") ?
				name :
				hasEmail ? vCard.getEmails().get(0).getValue() : vCard.getTelephoneNumbers().get(0).getText(), x -> x);
		toProcess.add(new ThreeTuple<>(cardId, card, vCard));
	}

	static void storeResourceMeta(Delta delta, ThreeTuple<String, DavResource, VCard> toProcess,
			Collection<String> groupURIs)
	{
		String cardId = toProcess.fst();
		DavResource card = toProcess.snd();
		VCard vCard = toProcess.thrd();

		if (!vCard.getExtendedProperties().isEmpty() && vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-KIND").get(0)
				.getValue().equals("group"))
		{
			groupURIs.add(cardId);
			for (RawProperty member : vCard.getExtendedProperties("X-ADDRESSBOOKSERVER-MEMBER"))
			{
				addAttribute(delta, cardId, Vocabulary.HAS_MEMBER_UID, member.getValue().replace("urn:uuid:", ""),
						x -> x);
			}
		}
		else
		{
			addAttribute(delta, cardId, Vocabulary.HAS_UID, vCard.getUid().getValue(), x -> x);
		}

		addAttribute(delta, cardId, Vocabulary.HAS_ETAG, card.getEtag(), x -> x);

	}

	// TODO :: SPLIT THIS UP TO NOT NOTIFY OF SOME DELETES
	static void unstoreRes(Model messageDB, Delta delta, String containerName, String resName)
	{
		Resource res = R(messageDB, resName);

		StmtIterator it = messageDB.listStatements(res, null, (RDFNode) null);
		while (it.hasNext())
		{
			Statement stmt = it.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(messageDB.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		delta.delete(messageDB.listStatements(res, null, (RDFNode) null))
				.delete(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.CONTAINS), res))
				.delete(messageDB.listStatements(null, P(messageDB, Vocabulary.HAS_CARD), res))
				.delete(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.HAS_GROUP), res))
				.delete(messageDB.listStatements(res, P(messageDB, Vocabulary.HAS_CARD), (RDFNode) null));

	}

	static void deleteGroup(Model db, Delta delta, String bookID, String groupID)
	{
		Resource groupRes = R(db, groupID);

		delta.delete(db.listStatements(groupRes, P(db, Vocabulary.HAS_NAME), (RDFNode) null))
				.delete(db.listStatements(groupRes, P(db, Vocabulary.RDF_TYPE), (RDFNode) null))
				.delete(db.listStatements(groupRes, P(db, Vocabulary.HAS_CARD), (RDFNode) null))
				.delete(db.listStatements(R(db, bookID), P(db, Vocabulary.HAS_GROUP), groupRes));

		System.out.println("DID ALL DELTA DELETES FOR A GROUP");

	}

	static void computeMemberCardDiffs(Model messageDB, Delta delta, String resourceURI, Collection<String> oldMem,
			Collection<String> updatedMem)
	{
		// Wil optimize this later lol, 2am coding
		Collection<String> membersToAdd = new ArrayList<>();
		Collection<String> membersToDelete = new ArrayList<>();

		for (String member : oldMem)
		{
			if (!updatedMem.contains(member))
			{
				membersToDelete.add(member);
			}
		}

		for (String member : updatedMem)
		{
			if (!oldMem.contains(member))
			{
				membersToAdd.add(member);
			}
		}

		for (String member : membersToAdd)
		{
			System.out.println("ADDED SOMEONE TO GROUP");
			addAttribute(delta, resourceURI, Vocabulary.HAS_CARD, member, x -> x);
		}

		for (String member : membersToDelete)
		{
			System.out.println("REMOVED SOMEONE FROM GROUP");
			delta.delete(messageDB.listStatements(R(messageDB, resourceURI), P(messageDB, Vocabulary.HAS_CARD),
					L(messageDB, member)));
		}
	}

	static void deleteVCard(Model db, Delta delta, String bookID, String cardID)
	{
		Resource cardRes = R(db, cardID);

		StmtIterator phones = db.listStatements(cardRes, P(db, Vocabulary.HAS_PHONE), (RDFNode) null);
		while (phones.hasNext())
		{
			Statement stmt = phones.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(db.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}

			delta.delete(db.listStatements(cardRes, P(db, Vocabulary.HAS_PHONE), stmt.getObject().asResource()));
		}

		StmtIterator emails = db.listStatements(cardRes, P(db, Vocabulary.HAS_PHONE), (RDFNode) null);
		while (emails.hasNext())
		{
			Statement stmt = emails.next();
			if (stmt.getObject().isResource())
			{
				delta.delete(db.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}

			delta.delete(db.listStatements(cardRes, P(db, Vocabulary.HAS_EMAIL), stmt.getObject().asResource()));
		}

		delta.delete(db.listStatements(cardRes, P(db, Vocabulary.HAS_NAME), (RDFNode) null))
				.delete(db.listStatements(cardRes, P(db, Vocabulary.RDF_TYPE), (RDFNode) null))
				.delete(db.listStatements(R(db, bookID), P(db, Vocabulary.CONTAINS), cardRes));

		System.out.println("DID ALL DELTA DELETES FOR A CARD");
	}

	static void unStoreMeta(Model db, Delta delta, String resName)
	{
		Resource davRes = R(db, resName);

		delta.delete(db.listStatements(davRes, P(db, Vocabulary.HAS_CTAG), (RDFNode) null))
				.delete(db.listStatements(davRes, P(db, Vocabulary.HAS_ETAG), (RDFNode) null))
				.delete(db.listStatements(davRes, P(db, Vocabulary.HAS_UID), (RDFNode) null))
				.delete(db.listStatements(davRes, P(db, Vocabulary.HAS_MEMBER_UID), (RDFNode) null));

	}
}
