package ir;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class CorpusDocumentCodec implements Codec<CorpusDocument>{

	@Override
	public void encode(BsonWriter writer, CorpusDocument doc, EncoderContext ctx) {
		writer.writeStartDocument();
		writer.writeInt32("did", doc.did);
		writer.writeString("name", doc.name);
		writer.writeInt32("lenght", doc.lenght);
		writer.writeEndDocument();		
	}

	@Override
	public Class<CorpusDocument> getEncoderClass() {
		return CorpusDocument.class;
	}

	@Override
	public CorpusDocument decode(BsonReader reader, DecoderContext ctx) {
		CorpusDocument doc = new CorpusDocument();
		
		reader.readStartDocument();
		
		doc.id = reader.readObjectId("_id");
		doc.did = reader.readInt32("did");
		doc.name = reader.readString("name");
		doc.lenght = reader.readInt32("lenght");
		
		reader.readEndDocument();
		
		return doc;
	}

}
