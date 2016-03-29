package ir;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;

import org.bson.types.ObjectId;

public class CorpusDocument implements Comparable<CorpusDocument>, Serializable{
	public ObjectId id;
	public Integer did;
	public String name;
	public Integer lenght;
	public Double rank;
	
    public PostingsTermsList terms = new PostingsTermsList();
	
	@Override
	public int compareTo(CorpusDocument other) {
		return Double.compare( other.rank, this.rank );
	}
	
	public static final Comparator<CorpusDocument> RANK_DESC = 
            new Comparator<CorpusDocument>() {
    			public int compare(CorpusDocument e1, CorpusDocument e2) {
    				return Double.compare( e1.rank, e2.rank );
    			}
    		};
}
