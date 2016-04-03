package ir;

import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class IndexEntryCodec implements Codec<IndexEntry> {

	@Override
	public void encode(BsonWriter writer, IndexEntry ie, EncoderContext ctx) {
		writer.writeStartDocument();
		writer.writeString("term", ie.token);
		writer.writeInt32("df", ie.df);
		
		writer.writeName("posts");		
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
		
		while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
			String fieldName = reader.readName();
			switch (reader.getCurrentBsonType()) {
		        case INT32:
		            ie.df = reader.readInt32();
		            break;
		        case STRING:
		        	ie.token = reader.readString();
		        	break;
		        case ARRAY:
		    		PostingsListCodec codec = new PostingsListCodec();
		    		ie.postings = codec.decode(reader, ctx);
		    		break;
		        case OBJECT_ID:
		        	ie.id = reader.readObjectId();
		        	break;
		    }
		}
		
		/*ie.id = reader.readObjectId("_id");
		ie.token = reader.readString("term");
		ie.df = reader.readInt32("df");
		
		System.out.println(reader.getCurrentBsonType());
		PostingsListCodec codec = new PostingsListCodec();
		ie.postings = codec.decode(reader, ctx);*/
		reader.readEndDocument();
		
		return ie;
	}

}
