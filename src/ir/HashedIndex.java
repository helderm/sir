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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import org.bson.types.ObjectId;

/**
 *   Implements an inverted index as a Hashtable from words to PostingsLists.
 */
public class HashedIndex implements Index, Iterable<Map.Entry<String, PostingsList>>{

    private LruCache<String,PostingsList> cache;
    
    private MongoDatabase db;
	private boolean flushCache;
	private Integer cacheSize;
	private Integer postingsMaxSize;
	private Corpus corpus;	
	
	// min and max tfidf scores, for data normalization
	private Double minScore = 99.0;
	private Double maxScore = -1.0;

    public HashedIndex(MongoDatabase db, Corpus corpus, Options opt) {
    	if(opt.cacheSize >= 0){    		
    		this.cacheSize = opt.cacheSize;
    	}else
    		this.cacheSize = LruCache.INFINITY;

    	this.cache = new LruCache<String,PostingsList>(this.cacheSize);
    	this.db = db;
    	
   		this.flushCache = opt.recreateIndex;
   		this.postingsMaxSize = opt.postingsMaxSize;
   		this.corpus = corpus;
	}

    /**
     *  Inserts this token in the index.
     */
    public void insert( String token, int docID, int offset ) {
	
		// first search the postings list in the cache
    	PostingsList postings = this.cache.get(token);
      	
		PostingsEntry posting = new PostingsEntry();
		posting.docID = docID;
		posting.score = 0.0;
		posting.positions.add(offset);
    	
    	if(postings != null){
    		postings.add(posting);
    	}else{    	
	    	// new token being added into the index
			postings = new PostingsList();        	
			postings.add(posting);		
	    	this.cache.put(token, postings);
	    	
	    	if(this.flushCache == true && this.cache.size() >= this.cacheSize){	
	       
	    		// flush cache to db
	    	    for(Map.Entry<String, PostingsList> map : this){
	    	    	savePostings(map.getKey(), map.getValue());
	        	}   
	    		
	    		// clean the cache
	    		this.cache = new LruCache<String,PostingsList>(this.cacheSize);	    		
	    	}
    	}		
    }

    public void savePostings(String token, PostingsList postings){
		Integer ptr = 0;

    	MongoCollection<IndexEntry> col = this.db.getCollection("index", IndexEntry.class);    	
    	MongoCursor<IndexEntry> it = col.find(eq("term", token)).iterator();
    	
    	ArrayList<ObjectId> entriesIds = new ArrayList<>();
    	PostingsList joinedPostings = new PostingsList();
    	
    	while(it.hasNext()){
    		IndexEntry ie = it.next();
    		joinedPostings.add(ie.postings);
    		entriesIds.add(ie.id);
    	}
    	
    	joinedPostings.add(postings);
    	Collections.sort(joinedPostings.getList());
    	    	
    	while(ptr < joinedPostings.size() && entriesIds.size() > 0){
    		ObjectId eid = entriesIds.get(0);
    		IndexEntry ie = new IndexEntry();
    		ie.id = eid;
    		ie.postings = joinedPostings.get(ptr, ptr + this.postingsMaxSize);  
    		ie.token = token;
    		ptr += this.postingsMaxSize;
    		entriesIds.remove(0);
    		col.findOneAndReplace(eq("_id", ie.id), ie);
    	}
    	
		// split into several smaller index entries if the term is too common	
		while(ptr < joinedPostings.size()){
			IndexEntry ie = new IndexEntry();
		   	ie.token = token;
		   	ie.postings = joinedPostings.get(ptr, ptr + this.postingsMaxSize);
	    	col.insertOne(ie);
	    	ptr += this.postingsMaxSize;
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
    	
    	pl = new PostingsList();
    	
    	// try to fetch from db
		try{
    		MongoCollection<IndexEntry> col = this.db.getCollection("index", IndexEntry.class);
        	MongoCursor<IndexEntry> it = col.find(eq("term", token)).iterator();
        	
        	// add postings to every index entry until it is filled
        	while(it.hasNext()){
        		IndexEntry ie = it.next();        		
        		pl.add(ie.postings);
        	}
        	
        	this.cache.put(token, pl);
    		
		}catch(Exception e){
    		System.err.println("Error while fetching token postings from db!");
     		System.err.println(e);
     		return new PostingsList();
    	}    	
    	
    	return pl;
    }


    /**
     *  Searches the index for postings matching the query.
     */
    public PostingsList search( Query query, int queryType, int rankingType, int structureType ) {

    	// create term-postings list, and sort by increasing doc frequency
    	ArrayList<Query.TermPostings> termsPostings = new ArrayList<Query.TermPostings>();
    	
    	int i;
    	for(i=0; i<query.terms.size(); i++){
    		String term = query.terms.get(i);
    		Double weight = query.weights.get(i);
    		
    		Query.TermPostings tp = query.new TermPostings();
    		tp.term = term;
    		tp.weight = weight;
    		tp.postings = getPostings(term);
    		termsPostings.add(tp);
    	}
    	
    	switch(queryType){
    	case Index.INTERSECTION_QUERY:
    		// reorder terms by increasing size of the postings list
    		Collections.sort(termsPostings);
    		
    	case Index.PHRASE_QUERY:
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
        	
    	case Index.RANKED_QUERY:    		
    		switch(rankingType){
    		case Index.PAGERANK:
    			return rankedQuery(termsPostings, 0.0, 1.0);
    		case Index.COMBINATION:
    			return rankedQuery(termsPostings, 0.65, 0.35);
    		case Index.TF_IDF:
    		default:
    			return rankedQuery(termsPostings, 1.0, 0.0);
    		}
    	
    	case Index.RELEVANCE_FEEDBACK_QUERY:
    		return cosineScore(termsPostings);    	
    	}  	
    	
    	return new PostingsList();

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
						Boolean found = false;
						for(PostingsEntry aux : answer){
							if(aux.docID == pe2.docID){
								found = true;
								aux.positions.add(l.get(0));
							}
						}
						if(found == false){
							PostingsEntry newPe = new PostingsEntry();
							newPe.positions.add(l.get(0));
							newPe.docID = pe2.docID;
							newPe.score = pe2.score;
							answer.add(newPe);							
						}
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
    
    public PostingsList rankedQuery(ArrayList<Query.TermPostings> query, Double tfidf, Double pagerank){
    	PostingsList answer = new PostingsList();
    	
    	// for each query term
    	for(Query.TermPostings tp : query){
    		// for each doc in the postings lists
    		for(PostingsEntry pe : tp.postings){
    			PostingsEntry ape = new PostingsEntry(pe);
    			CorpusDocument doc = this.corpus.getDocument(pe.docID);
    			
    			Double score = (pe.score - getMinScore()) / (getMaxScore() - getMinScore());
    			Double rank = (doc.rank - this.corpus.getMinRank()) / (this.corpus.getMaxRank() - this.corpus.getMinRank());
    			
    			ape.score = (tfidf * score) + (pagerank * rank);
    			answer.add(ape);
    		}
    	}
    	
    	Collections.sort(answer.getList(), PostingsEntry.SCORE_ORDER);    	
    	return answer;
    	
    }
    
    public PostingsList cosineScore(ArrayList<Query.TermPostings> query){
    	PostingsList answer = new PostingsList();
    	
    	// for each query term
    	for(Query.TermPostings tp : query){
    		
    		// for each doc in the postings lists
    		for(PostingsEntry pe : tp.postings){
    			PostingsEntry ape = new PostingsEntry(pe);
    			
    			Double score = pe.score * tp.weight;
    			ape.score = score;
    			answer.add(ape);
    		}
    	}
    	
    	Collections.sort(answer.getList(), PostingsEntry.SCORE_ORDER);      	
    	
    	return answer;
    }
    
	/**
	 * Calculate the TF-IDF scores for every term in the db
	 */
    public void calculateScores() {
		
		MongoCollection<IndexEntry> col = this.db.getCollection("index", IndexEntry.class);
    	MongoCursor<IndexEntry> it = col.find().iterator();

    	HashSet<String> updatedTerms = new HashSet<>();
    	Double minScore = 1.0;
    	Double maxScore = 0.0;
    	
    	// for every term in the db
    	int counter = 0;
    	while(it.hasNext()){
    		IndexEntry ie = it.next();        		
    		
    		// we may have same term in different docs, so we dont have to recalculate
    		//  it again for the terms that we already did it
    		if(updatedTerms.contains(ie.token))
    			continue;    		
    		
    		PostingsList postings = getPostings(ie.token);
    		
    		Double idf = this.corpus.idf(postings);
    		for(PostingsEntry pe : postings){
    			CorpusDocument doc = this.corpus.getDocument(pe.docID);
    			
    			// calculate the lenght normalized tf-idf score
    			pe.score = this.corpus.tf(pe) * idf;
    			pe.score /= Math.sqrt(doc.lenght);    
    			
    			PostingsTermEntry pte = new PostingsTermEntry();
    			pte.term = ie.token;
    			pte.score = pe.score;    			
    			doc.terms.add(pte);
    			this.corpus.insertDocument(doc);
    			
    			if(pe.score < minScore)
    				minScore = pe.score;
    			if(pe.score > maxScore)
    				maxScore = pe.score;
    		}
    		
    		savePostings(ie.token, postings);
    		updatedTerms.add(ie.token);
    		
    		if(++counter % 1000 == 0)
    			System.out.println("[" + counter + "] terms processed...");
    	}
    	
    	// add the max min tfidf scores, for normalization    	
    	MongoCollection<Document> scCol = this.db.getCollection("scores");
    	Document score = new Document();
    	score.append("type", "tfidf");
    	score.append("max", maxScore).append("min", minScore);
    	scCol.insertOne(score);

	}
    
	public Double getMinScore(){
		if(this.minScore < 99.0)
			return this.minScore;
		
		MongoCollection<Document> col = this.db.getCollection("scores");		
		Document doc = col.find(eq("type", "tfidf")).first();
		
		this.minScore = doc.getDouble("min");
		return this.minScore;
	}
	
	public Double getMaxScore(){
		if(this.maxScore > -1.0)
			return this.maxScore;
		
		MongoCollection<Document> col = this.db.getCollection("scores");		
		Document doc = col.find(eq("type", "tfidf")).first();
		
		this.maxScore = doc.getDouble("max");
		return this.maxScore;
	}
    
    public void cleanup() {
    }

	@Override
	public Iterator<Map.Entry<String, PostingsList>> iterator() {
		return this.cache.entrySet().iterator();
	}
	
}
