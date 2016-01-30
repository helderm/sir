/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 *   Additions: Hedvig Kjellström, 2012-14
 */  


package ir;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import ir.Query.TermPostings;


/**
 *   Implements an inverted index as a Hashtable from words to PostingsLists.
 */
public class HashedIndex implements Index, Serializable {

	/** The index as a hashtable. */
    private HashMap<String,PostingsList> index;
    
    // copied from the interface declaration, to serialize it too
    private HashMap<String, String> docIDs = new HashMap<String,String>();
    private HashMap<String,Integer> docLengths = new HashMap<String,Integer>(); 
    
	private static final long serialVersionUID = 2L;

    public HashedIndex() {
    	this.index = new HashMap<String,PostingsList>();
	}

    /**
     *  Inserts this token in the index.
     */
    public void insert( String token, int docID, int offset ) {
    	PostingsList postings = this.index.get(token);
  	
		PostingsEntry posting = new PostingsEntry();
		posting.docID = docID;
		posting.score = 1;
		posting.positions.add(offset);
    	
    	if(postings != null){
    		postings.add(posting);
    	}else{    	
	    	// new token being added into the index
			postings = new PostingsList();        	
			postings.add(posting);		
	    	this.index.put(token, postings);
    	}
   	
    }


    /**
     *  Returns all the words in the index.
     */
    public Iterator<String> getDictionary() {
	// 
	//  REPLACE THE STATEMENT BELOW WITH YOUR CODE
	//
	return null;
    }


    /**
     *  Returns the postings for a specific term, or null
     *  if the term is not in the index.
     */
    public PostingsList getPostings( String token ) {
    	return this.index.get(token);
    }


    /**
     *  Searches the index for postings matching the query.
     */
    public PostingsList search( Query query, int queryType, int rankingType, int structureType ) {

    	// create term-postings list, and sort by increasing doc frequency
    	ArrayList<Query.TermPostings> termsPostings = new ArrayList<Query.TermPostings>();
    	for(String term : query.terms){
    		Query.TermPostings tp = query.new TermPostings();
    		tp.term = term;
    		tp.postings = getPostings(term);
    		termsPostings.add(tp);
    	}
    	if(queryType != Index.PHRASE_QUERY)
    		Collections.sort(termsPostings);

    	int ptr = 1;
    	PostingsList result = termsPostings.get(0).postings;
    	
    	while(result.size() > 0 && ptr < termsPostings.size()){
    		if(queryType == Index.PHRASE_QUERY)
    			result = positionalIntersect(result, termsPostings.get(ptr).postings, 1);
    		else
    			result = intersect(result, termsPostings.get(ptr).postings);
    		ptr++;    		
    	}
    	
    	return result;
    }
    
    private PostingsList intersect(PostingsList pl1, PostingsList pl2){
    	PostingsList answer = new PostingsList();
    	
    	Iterator it1 = pl1.iterator();
    	Iterator it2 = pl2.iterator();    	

		PostingsEntry pe1 = (PostingsEntry) it1.next();
		PostingsEntry pe2 = (PostingsEntry) it2.next();    	
    	
		while(true){
    		if(pe1.docID == pe2.docID){
    			answer.add(pe1);
        		
    			if(it1.hasNext() == false || it2.hasNext() == false)
    				break;
    			
    			pe1 = (PostingsEntry) it1.next();
        		pe2 = (PostingsEntry) it2.next();
        		continue;
    		}
    		
    		if(pe1.docID < pe2.docID){
    			if(it1.hasNext() == false)
    				break;
    			pe1 = (PostingsEntry) it1.next();
    		} else {
       			if(it2.hasNext() == false)
    				break;
    			pe2 = (PostingsEntry) it2.next();
    		}
    	}
    	
    	return answer;
    }

    public PostingsList positionalIntersect(PostingsList pl1, PostingsList pl2, int distance){
    	PostingsList answer = new PostingsList();
    	
    	Iterator it1 = pl1.iterator();
    	Iterator it2 = pl2.iterator();    	

		PostingsEntry pe1 = (PostingsEntry) it1.next();
		PostingsEntry pe2 = (PostingsEntry) it2.next();    	
    	
		while(true){
    		if(pe1.docID == pe2.docID){
    			
    	    	Iterator pp1 = pe1.positions.iterator();
    			ArrayList<Integer> l = new ArrayList<Integer>();
    			
    			while(pp1.hasNext()){
					int pos1 = (int) pp1.next();
					Iterator pp2 = pe2.positions.iterator();
					
					while(pp2.hasNext()){
    					int pos2 = (int) pp2.next();
    					
    					if(pos1 < pos2 && Math.abs(pos1 - pos2) <= distance)
    						l.add(pos2);
    					else if(pos2 > pos1)
    						break;    						
    				}
					
					while(l.isEmpty() == false && (Math.abs(l.get(0) - pos1) > distance))
						l.remove(0);
					
					if(l.isEmpty() == false){
						answer.add(pe1);
						break;
					}
    			}
        		
    			if(it1.hasNext() == false || it2.hasNext() == false)
    				break;
    			
    			pe1 = (PostingsEntry) it1.next();
        		pe2 = (PostingsEntry) it2.next();
        		continue;
    		}
    		
    		if(pe1.docID < pe2.docID){
    			if(it1.hasNext() == false)
    				break;
    			pe1 = (PostingsEntry) it1.next();
    		} else {
       			if(it2.hasNext() == false)
    				break;
    			pe2 = (PostingsEntry) it2.next();
    		}
    	}    	
    	
    	return answer;
    }
    

    /**
     *  No need for cleanup in a HashedIndex.
     */
    public void cleanup() {
    }

	@Override
	public HashMap<String, String> getDocIDs() {
		// TODO Auto-generated method stub
		return this.docIDs;
	}

	@Override
	public void addDocID(String docID, String filepath) {
		this.docIDs.put( docID, filepath );		
	}

	@Override
	public HashMap<String, Integer> getDocLenghts() {
		// TODO Auto-generated method stub
		return this.docLengths;
	}

	@Override
	public void addDocLenght(String docID, Integer lenght) {
		// TODO Auto-generated method stub
		this.docLengths.put(docID, lenght);
	}
}
