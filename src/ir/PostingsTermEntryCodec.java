package ir;

import java.util.ArrayList;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class PostingsTermEntryCodec implements Codec<PostingsTermEntry>{
	@Override
	public void encode(BsonWriter writer, PostingsTermEntry pe, EncoderContext ctx) {
		writer.writeStartDocument();
		writer.writeString("t", pe.term);
		writer.writeDouble("s", pe.score);
		writer.writeEndDocument();
	}

	@Override
	public Class<PostingsTermEntry> getEncoderClass() {
		return PostingsTermEntry.class;
	}

	@Override
	public PostingsTermEntry decode(BsonReader reader, DecoderContext ctx) {
		PostingsTermEntry pe = new PostingsTermEntry();
		
		reader.readStartDocument();
		pe.term = reader.readString("t");
		pe.score = reader.readDouble("s");
		reader.readEndDocument();		

		return pe;
	}
}
