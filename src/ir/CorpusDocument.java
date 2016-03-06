package ir;

import org.bson.types.ObjectId;

public class CorpusDocument implements Comparable<CorpusDocument>{
	public ObjectId id;
	public Integer did;
	public String name;
	public Integer lenght;
	public Double rank;
	
	@Override
	public int compareTo(CorpusDocument other) {
		return Double.compare( other.rank, this.rank );
	}
}
