package ir;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class PostingsListCodec implements Codec<PostingsList> {
	
	@Override
	public void encode(BsonWriter writer, PostingsList pl, EncoderContext ctx) {
		PostingsEntryCodec codec = new PostingsEntryCodec();
		
	    writer.writeStartArray();
	    
	    for(PostingsEntry pe : pl ){
	    	codec.encode(writer, pe, ctx);
	    }
 
	    writer.writeEndArray();
	}

	@Override
	public Class<PostingsList> getEncoderClass() {
		return PostingsList.class;
	}

	@Override
	public PostingsList decode(BsonReader reader, DecoderContext ctx) {
		PostingsList pl = new PostingsList();
		
		reader.readStartArray();
	    
		PostingsEntryCodec codec = new PostingsEntryCodec();
		while (reader.readBsonType() != BsonType.END_OF_DOCUMENT){
			PostingsEntry pe = codec.decode(reader, ctx);
			pl.add(pe);			
		}
  
	    reader.readEndArray();	
	    return pl;
	}
}
