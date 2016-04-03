package ir;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class BiwordIndex extends BaseIndex {

	private String previousToken;
	private int currDocID = -1;
	
	public BiwordIndex(MongoDatabase db, Corpus corpus, Options opt) {
		super(db, corpus, opt);
		
	}

	@Override
	public void insert(String token, int docID, int offset) {
		if(currDocID != docID){
			previousToken = token;
			currDocID = docID;
			return;
		}
		
		String bigram = previousToken + " " + token;
		super.insert(bigram, docID, offset-1);
		
		previousToken = token;
	}
	
	@Override
	public void calculateScores(){
		MongoCollection<IndexEntry> col = this.db.getCollection(this.col(), IndexEntry.class);
    	MongoCursor<IndexEntry> it = col.find().noCursorTimeout(true).iterator();
   	
    	// for every term in the db
    	try{
	    	int counter = 0;
	    	while(it.hasNext()){
	    		IndexEntry ie = it.next();        		
	    		
	    		PostingsList postings = getPostings(ie.token, false);
	    		
	    		Double idf = this.corpus.idf(postings);
	    		for(PostingsEntry pe : postings){
	    			CorpusDocument doc = this.corpus.getDocument(pe.docID);
	    			
	    			// calculate the lenght normalized tf-idf score
	    			pe.score = this.corpus.tf(pe) * idf;
	    			pe.score /= Math.sqrt(doc.lenght);    
	    		}
	    		
	    		savePostings(ie.token, postings);
	    		
	    		if(++counter % 10000 == 0)
	    			System.out.println("- [" + counter + "] terms scored...");
	    	}
    	} catch(Exception e){
    		System.err.print(e);
    	} 
    	
    	it.close();
	}
	
	@Override
	public String col(){
		return "bindex";
	}

	@Override
	public Double getGramWeight(){
		return 1.2;
	}
	
}
