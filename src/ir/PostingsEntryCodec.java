package ir;

import java.util.ArrayList;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class PostingsEntryCodec implements Codec<PostingsEntry> {

	@Override
	public void encode(BsonWriter writer, PostingsEntry pe, EncoderContext ctx) {
		writer.writeStartDocument();
		writer.writeInt32("docID", pe.docID);
		writer.writeDouble("score", pe.score);
		writer.writeStartArray("positions");			
		
		for(Integer pos : pe.positions)
			writer.writeInt32(pos);
		
		writer.writeEndArray();
		writer.writeEndDocument();
	}

	@Override
	public Class<PostingsEntry> getEncoderClass() {
		return PostingsEntry.class;
	}

	@Override
	public PostingsEntry decode(BsonReader reader, DecoderContext ctx) {
		PostingsEntry pe = new PostingsEntry();
		
		reader.readStartDocument();
		pe.docID = reader.readInt32("docID");
		pe.score = reader.readDouble("score");
		pe.positions = new ArrayList<Integer>();
		
		reader.readStartArray();
		while (reader.readBsonType() != BsonType.END_OF_DOCUMENT){
			Integer pos = reader.readInt32();
			pe.positions.add(pos);
		}
		
		reader.readEndArray();
		reader.readEndDocument();		

		return pe;
	}

}
