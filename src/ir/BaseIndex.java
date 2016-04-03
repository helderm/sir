package ir;

import static com.mongodb.client.model.Filters.eq;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.bson.BSON;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Projections.*;

public class BaseIndex implements Index, Iterable<Map.Entry<String, PostingsList>> {
    
	private static final String DEFAULT_IDX_COL_NAME = "index";
	private static final Double QUERY_SCORE_MIN_THRESHOLD = 0.1;
	private static final Integer CHAMPIONS_LIST_NUM_DOCS = 30;
	
	private Integer cacheSize;
	private LruCache<String, PostingsList> cache;
	public MongoDatabase db;
	private Boolean flushCache;
	private Integer postingsMaxSize;
	private Boolean useSpeedup;
	public Corpus corpus;

	// min and max tfidf scores, for data normalization
	private Double minScore = 99.0;
	private Double maxScore = -1.0;
	
	public BaseIndex(MongoDatabase db, Corpus corpus, Options opt) {
    	if(opt.cacheSize >= 0){    		
    		this.cacheSize = opt.cacheSize;
    	}else
    		this.cacheSize = LruCache.INFINITY;

    	this.cache = new LruCache<String,PostingsList>(this.cacheSize);
    	this.db = db;
    	
   		this.flushCache = opt.recreateIndex;
   		this.postingsMaxSize = opt.postingsMaxSize;
   		this.useSpeedup = opt.useSpeedup;
   		this.corpus = corpus;
	}
	
	@Override
	public void insert(String token, int docID, int offset) {
		
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
	    		save();    		
	    	}
    	}	

	}
	
	@Override
	public void save(){
		/** Save every entry in cache into DB, then flush the cache */
		
	    int totalEntries = this.cache.size();
	    int count = 0;
		for(Map.Entry<String, PostingsList> map : this){
	    	savePostings(map.getKey(), map.getValue());
	    	if(++count % 10000 == 0)
	    		System.out.println("- ["+count+"] out of ["+totalEntries+"] inserted...");
	    	
    	}   
		
		// clean the cache
		this.cache = new LruCache<String,PostingsList>(this.cacheSize);	  		
	}
	
    public void savePostings(String token, PostingsList postings){
		Integer ptr = 0;

    	MongoCollection<IndexEntry> col = this.db.getCollection(this.col(), IndexEntry.class);    	
    	MongoCursor<IndexEntry> it = col.find(eq("term", token)).iterator();
    	
    	ArrayList<ObjectId> entriesIds = new ArrayList<>();
    	PostingsList joinedPostings = new PostingsList();
    	
    	while(it.hasNext()){
    		IndexEntry ie = it.next();
    		joinedPostings.add(ie.postings);
    		entriesIds.add(ie.id);
    	}
    	
    	joinedPostings.add(postings);
    	
    	int df = joinedPostings.size();
    	Collections.sort(joinedPostings.getList());
    	    	
    	while(ptr < joinedPostings.size() && entriesIds.size() > 0){
    		ObjectId eid = entriesIds.get(0);
    		IndexEntry ie = new IndexEntry();
    		ie.id = eid;
    		ie.postings = joinedPostings.get(ptr, ptr + this.postingsMaxSize);  
    		ie.token = token;
    		ie.df = df;
    		ptr += this.postingsMaxSize;
    		entriesIds.remove(0);
    		col.findOneAndReplace(eq("_id", ie.id), ie);
    	}
    	
		// split into several smaller index entries if the term is too common	
		while(ptr < joinedPostings.size()){
			IndexEntry ie = new IndexEntry();
		   	ie.token = token;
		   	ie.postings = joinedPostings.get(ptr, ptr + this.postingsMaxSize);
		   	ie.df = df;
	    	col.insertOne(ie);
	    	ptr += this.postingsMaxSize;
		}
    }

	@Override
	public Iterator<String> getDictionary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PostingsList getPostings(String token, boolean useChampions) {
    	PostingsList pl;
		IndexEntry ie;
    	
    	if(this.db == null)
    		return new PostingsList();
    	
    	pl = new PostingsList();
    	
		// try fetching first from the champions list
		if(useChampions){
    		MongoCollection<IndexEntry> chCol = this.db.getCollection("champions", IndexEntry.class);
    		ie = chCol.find(eq("term", token)).first();
    		if(ie != null){
    			System.out.println("Returning champions list for term ["+token+"]...");
    			pl.add(ie.postings);
    			return pl;
    		}
		}
    	
    	// try to fetch from db
		try{
    		MongoCollection<IndexEntry> col = this.db.getCollection(this.col(), IndexEntry.class);

    		// return the postings list from the cache if we have it stored
    		if(this.cache.containsKey(token))
    			return this.cache.get(token);
    		
    		MongoCursor<IndexEntry> it = col.find(eq("term", token)).iterator();
        	
        	// add postings to every index entry until it is filled
        	while(it.hasNext()){
        		ie = it.next();        		
        		pl.add(ie.postings);
        	}
        	
        	this.cache.put(token, pl);
    		
		}catch(Exception e){
    		System.err.println("Error while fetching token postings from db!");
     		return new PostingsList();
    	}    	
    	
    	return pl;
	}

    /**
     *  Searches the index for postings matching the query.
     */
    public PostingsList search( Query query, int queryType, int rankingType) {

    	// create term-postings list, and sort by increasing doc frequency
    	ArrayList<Query.TermPostings> termsPostings = new ArrayList<Query.TermPostings>();
    	
    	int i;
    	for(i=0; i<query.terms.size(); i++){
    		String term = query.terms.get(i);
    		Double weight = query.weights.get(i);
    		
    		// idx elmination: ignore query terms with too low of a score
    		if(this.useSpeedup && weight < QUERY_SCORE_MIN_THRESHOLD){
    			System.out.println("Term ["+term+"] ignored, score too low! ("+weight+")");
    			continue;
    		}
    		
    		Query.TermPostings tp = new Query.TermPostings();
    		tp.term = term;
    		tp.weight = weight;
    		tp.postings = getPostings(term, this.useSpeedup);    		
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
    		return rankedQuery(termsPostings, 1.0, 0.0);    	
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
    		    			
    			Double score = 0.0;
    			Double rank = 0.0;
    			if(tfidf > 0.0)
    				//score = ((pe.score * tp.weight) - getMinScore()) / (getMaxScore() - getMinScore());
    				score = pe.score * tp.weight * getGramWeight();
    			if(pagerank > 0.0){
    				CorpusDocument doc = this.corpus.getDocument(pe.docID);
    				//rank = (doc.rank - this.corpus.getMinRank()) / (this.corpus.getMaxRank() - this.corpus.getMinRank());
    				rank = doc.rank;
    			}
    			
    			ape.score = (tfidf * score) + (pagerank * rank);
    			answer.add(ape);
    		}
    	}
    	
    	Collections.sort(answer.getList(), PostingsEntry.SCORE_ORDER);    	
    	return answer;
    	
    }

	/**
	 * Calculate the TF-IDF scores for every term in the db
	 */
    @Override
    public void calculateScores() {
		
		MongoCollection<IndexEntry> col = this.db.getCollection(this.col(), IndexEntry.class);
    	MongoCursor<IndexEntry> it = col.find().noCursorTimeout(true).iterator();

    	HashSet<String> updatedTerms = new HashSet<>();
    	Double minScore = 1.0;
    	Double maxScore = 0.0;
    	
    	// for every term in the db
    	try{
	    	
	    	int counter = 0;
	    	while(it.hasNext()){
	    		IndexEntry ie = it.next();        		
	    		
	    		// we may have same term in different docs in mongodb, so we dont have to recalculate
	    		//  it again for the terms that we already did it
	    		if(updatedTerms.contains(ie.token))
	    			continue;    		
	    		
	    		PostingsList postings = getPostings(ie.token, false);
	    		Integer df = getDf(ie.token);
	    		
	    		Double idf = this.corpus.idf(df);
	    		for(PostingsEntry pe : postings){
	    			CorpusDocument doc = this.corpus.getDocument(pe.docID);
	    			
	    			// calculate the lenght normalized tf-idf score
	    			pe.score = this.corpus.tf(pe) * idf;
	    			pe.score /= Math.sqrt(doc.lenght);    
	    			
	    			// create a entry in the inverted inverted index
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
	    		saveChampionsList(ie.token, postings);
	    		
	    		updatedTerms.add(ie.token);
	    		
	    		if(++counter % 1000 == 0)
	    			System.out.println("- [" + counter + "] terms scored...");
	    	}
    	} catch(Exception e){
    		 e.printStackTrace(new PrintStream(System.err));
    	} 
    	
    	it.close();
	    	
    	// add the max min tfidf scores, for normalization    	
    	MongoCollection<Document> scCol = this.db.getCollection("scores");
    	Document score = new Document();
    	score.append("type", "tfidf");
    	score.append("max", maxScore).append("min", minScore);
    	scCol.insertOne(score);

	}
    
    private void saveChampionsList(String term, PostingsList postings){
    	if(postings.size() < CHAMPIONS_LIST_NUM_DOCS)
    		return;
    	
    	Integer df = postings.size();
    	
    	MongoCollection<IndexEntry> col = this.db.getCollection("champions", IndexEntry.class); 
    	
    	// create a ordered copy of the postings list
    	PostingsList champions = new PostingsList(postings.getList());
    	Collections.sort(champions.getList(), PostingsEntry.SCORE_ORDER);
    	
    	// truncate
    	champions = champions.get(0, CHAMPIONS_LIST_NUM_DOCS);
    	
    	IndexEntry ie = new IndexEntry();
    	ie.token = term;
    	ie.postings = champions;
    	ie.df = df;
    	col.insertOne(ie);
    	
    }
    
    public Integer getDf(String term){
    	MongoCollection<IndexEntry> col = this.db.getCollection(this.col(), IndexEntry.class);
    	
    	if(this.cache.containsKey(term)){
    		return this.cache.get(term).size();
    	}
    		
    	FindIterable<IndexEntry> fie = col.find(eq("term", term));
    	if(this.col().equals(DEFAULT_IDX_COL_NAME))
    		fie.projection(fields(include("term","df")));
    	IndexEntry ie = fie.first();
    	if(ie == null)
    		return 0;
    	
    	return ie.df;
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

	@Override
	public void cleanup() {
	}
	
	public String col(){
		return DEFAULT_IDX_COL_NAME;
	}
	
	public Double getGramWeight(){
		return 1.0;
	}
	
	
	@Override
	public Iterator<Map.Entry<String, PostingsList>> iterator() {
		return this.cache.entrySet().iterator();
	}

}
