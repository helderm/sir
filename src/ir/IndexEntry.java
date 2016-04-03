package ir;

import org.bson.types.ObjectId;

public class IndexEntry {
	public String token;
	public Integer df = 0;
	public PostingsList postings;
	public ObjectId id;
}
