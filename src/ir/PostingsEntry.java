/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 */  

package ir;

import java.util.ArrayList;
import java.util.Comparator;

public class PostingsEntry implements Comparable<PostingsEntry> {
    
    public int docID;
    public double score;
    
    public ArrayList<Integer> positions = new ArrayList<Integer>();
    
    public static final Comparator<PostingsEntry> SCORE_ORDER = 
            new Comparator<PostingsEntry>() {
    			public int compare(PostingsEntry e1, PostingsEntry e2) {
    				return Double.compare( e2.score, e1.score );
    			}
    		};

    /**
     *  PostingsEntries are compared by their score (only relevant 
     *  in ranked retrieval).
     *
     *  The comparison is defined so that entries will be put in 
     *  descending order.
     */
    public int compareTo( PostingsEntry other ) {
    	return Integer.compare(this.docID, other.docID);
    	//return Double.compare( other.score, score );
    }

}

    
