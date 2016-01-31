package ir;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class IndexEntryCodec implements Codec<IndexEntry> {

	@Override
	public void encode(BsonWriter writer, IndexEntry ie, EncoderContext ctx) {
		writer.writeStartDocument();
		writer.writeString("token", ie.token);
		
		writer.writeName("postings");		
		PostingsListCodec codec = new PostingsListCodec();
		codec.encode(writer, ie.postings, ctx);
		
		writer.writeEndDocument();
	}

	@Override
	public Class<IndexEntry> getEncoderClass() {
		return IndexEntry.class;
	}

	@Override
	public IndexEntry decode(BsonReader reader, DecoderContext ctx) {
		IndexEntry ie = new IndexEntry();
		
		reader.readStartDocument();
		
		ie.id = reader.readObjectId("_id");
		ie.token = reader.readString("token");
		PostingsListCodec codec = new PostingsListCodec();
		ie.postings = codec.decode(reader, ctx);
		reader.readEndDocument();
		
		return ie;
	}

}
