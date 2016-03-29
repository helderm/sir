package ir;

import java.util.ArrayList;

public class PostingsTermEntry {
	public String term;
	public double score;
	
    public PostingsTermEntry(PostingsTermEntry pe) {
		this.term = pe.term;
		this.score = pe.score;
	}
    
    public PostingsTermEntry() {	
	}

    public int compareTo( PostingsEntry other ) {
    	return Double.compare( other.score, score );
    }
}
