/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   First version:  Johan Boye, 2010
 *   Second version: Johan Boye, 2012
 *   Additions: Hedvig Kjellström, 2012-14
 */  


package ir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

/**
 *   Implements an inverted index as a Hashtable from words to PostingsLists.
 */
public class HashedIndex implements Index, Iterable<Map.Entry<String, PostingsList>>{

    private LruCache<String,PostingsList> cache;
    
    private MongoDatabase db;

    public HashedIndex(MongoDatabase db, Options opt) {
    	if(opt.cacheSize >= 0)
    		this.cache = new LruCache<String,PostingsList>(opt.cacheSize);
    	else
    		this.cache = new LruCache<String,PostingsList>();

    	this.db = opt.memoryOnly? null:db;		
	}

    /**
     *  Inserts this token in the index.
     */
    public void insert( String token, int docID, int offset ) {
	
		// first search the postings list in the cache
    	PostingsList postings = this.cache.get(token);
      	
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
	    	this.cache.put(token, postings);
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
    	PostingsList pl = this.cache.get(token);
    	
    	if(pl != null)
    		return pl;
    	
    	if(this.db == null)
    		return new PostingsList();
    	
    	// try to fetch from db
		try{
    		MongoCollection<IndexEntry> col = this.db.getCollection("index", IndexEntry.class);
    		IndexEntry ie = col.find(eq("token", token)).first();
    		if(ie == null)
    			return new PostingsList();
    		
    		pl = ie.postings;
    		this.cache.put(token, pl);
		}catch(Exception e){
    		System.err.println("Error while fetching token postings from db!");
     		System.err.println(e);
    	}    	
    	
    	return pl;
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
    

    public void cleanup() {
    }

	@Override
	public Iterator<Map.Entry<String, PostingsList>> iterator() {
		return this.cache.entrySet().iterator();
	}
	
	public HashMap<String, String> getDocsInfo(PostingsList pl){
		HashMap<String, String> docsInfo = new HashMap<>();
		
		MongoCollection<Document> col = this.db.getCollection("docs");
		for(PostingsEntry pe : pl ){
			Document doc = col.find(eq("did", Integer.toString(pe.docID))).first();
			if(doc == null)
				continue;
			
			docsInfo.put(doc.getString("did"), doc.getString("name"));
		}
		
		return docsInfo;
	}
}
