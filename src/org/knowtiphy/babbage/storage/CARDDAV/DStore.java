package org.knowtiphy.babbage.storage.CARDDAV;

import biweekly.component.VEvent;
import com.github.sardine.DavResource;
import ezvcard.VCard;
import ezvcard.property.Email;
import ezvcard.property.Telephone;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelCon;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.knowtiphy.babbage.storage.Vocabulary;

import java.util.function.Function;
import java.util.function.Predicate;

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

	static <T> Literal L(Model model, T value, RDFDatatype rdfDatatype)
	{
		return model.createTypedLiteral(value, rdfDatatype);
	}

	static <S> void attr(Model model, Resource subject, String predicate, S value, Function<S, ? extends Literal> fn)
	{
		if (value != null)
		{
			model.add(subject, P(model, predicate), fn.apply(value));
		}
	}

	static <S> S optionalAttr(VEvent vEvent, Predicate<VEvent> predicate, Function<VEvent, S> fn)
	{
		if (predicate.test(vEvent))
		{
			return fn.apply(vEvent);
		}
		else
		{
			return null;
		}
	}

	static void storeAddressBook(Model model, String adapterID, String encodedAddressBook, DavResource addressBook)
	{
		Resource bookRes = model.createResource(encodedAddressBook);
		model.add(bookRes, model.createProperty(Vocabulary.RDF_TYPE),
				model.createResource(Vocabulary.CARDDAV_ADDRESSBOOK));
		model.add(model.createResource(adapterID), model.createProperty(Vocabulary.CONTAINS), bookRes);
		model.add(bookRes, model.createProperty(Vocabulary.HAS_NAME),
				model.createTypedLiteral(addressBook.getDisplayName()));
		model.add(bookRes, model.createProperty(Vocabulary.HAS_CTAG),
				model.createTypedLiteral(addressBook.getCustomProps().get("getctag")));
	}

	static void storeCard(Model addModel, String addressBookName, String cardName, VCard vCard, DavResource card)
	{
		Resource cardRes = R(addModel, cardName);
		addModel.add(cardRes, P(addModel, Vocabulary.RDF_TYPE), addModel.createResource(Vocabulary.CARDDAV_CARD));
		addModel.add(R(addModel, addressBookName), P(addModel, Vocabulary.CONTAINS), cardRes);

		attr(addModel, cardRes, Vocabulary.HAS_ETAG, card.getEtag(), x -> L(addModel, x));

		attr(addModel, cardRes, Vocabulary.HAS_FORMATTED_NAME, vCard.getFormattedName().getValue(), x -> L(addModel, x));

		for(Telephone telephone : vCard.getTelephoneNumbers())
		{
			String phoneNumber = telephone.getText();
			Resource phoneNumRes = R(addModel, phoneNumber);

			attr(addModel, cardRes, Vocabulary.HAS_NUMBER, phoneNumber, x -> L(addModel, x));
			attr(addModel, phoneNumRes, Vocabulary.HAS_NUMBER_TYPE, telephone.getTypes().get(0), x -> L(addModel, x));
		}

		for (Email email : vCard.getEmails())
		{
			String emailAddress = email.getValue();
			Resource emailAddressRes = R(addModel, emailAddress);

			attr(addModel, cardRes, Vocabulary.HAS_EMAIL, emailAddress, x -> L(addModel, x));
			attr(addModel, emailAddressRes, Vocabulary.HAS_EMAIL_TYPE, email.getTypes().get(0), x -> L(addModel, x));
		}


	}

	//  TODO -- have to delete the CIDS, content, etc
	static void unstoreRes(Model messageDB, Model deletes, String containerName, String resName)
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
				deletes.add(messageDB.listStatements(stmt.getObject().asResource(), null, (RDFNode) null));
			}
		}
		deletes.add(messageDB.listStatements(res, null, (RDFNode) null));
		deletes.add(messageDB.listStatements(R(messageDB, containerName), P(messageDB, Vocabulary.CONTAINS), res));
	}
}
