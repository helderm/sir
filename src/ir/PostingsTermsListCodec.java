package ir;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class PostingsTermsListCodec implements Codec<PostingsTermsList>{
	
	@Override
	public void encode(BsonWriter writer, PostingsTermsList pl, EncoderContext ctx) {
		PostingsTermEntryCodec codec = new PostingsTermEntryCodec();
		
	    writer.writeStartArray();
	    
	    for(PostingsTermEntry pe : pl ){
	    	codec.encode(writer, pe, ctx);
	    }
 
	    writer.writeEndArray();
	}

	@Override
	public Class<PostingsTermsList> getEncoderClass() {
		return PostingsTermsList.class;
	}

	@Override
	public PostingsTermsList decode(BsonReader reader, DecoderContext ctx) {
		PostingsTermsList pl = new PostingsTermsList();
		
		reader.readStartArray();
	    
		PostingsTermEntryCodec codec = new PostingsTermEntryCodec();
		while (reader.readBsonType() != BsonType.END_OF_DOCUMENT){
			PostingsTermEntry pe = codec.decode(reader, ctx);
			pl.add(pe);			
		}
  
	    reader.readEndArray();	
	    return pl;
	}
}
