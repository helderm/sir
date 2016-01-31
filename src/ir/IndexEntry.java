package ir;

import org.bson.types.ObjectId;

public class IndexEntry {
	public String token;
	public PostingsList postings;
	public ObjectId id;
}
