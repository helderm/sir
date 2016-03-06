package ir;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.HashMap;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Corpus {
	private Long size;
	private LruCache<Integer, CorpusDocument> cache;
	private MongoDatabase db;
    private int lastDocID = 0;
    
    // min and max pagerank scores,for data normalization
    private Double minRank = 1.0;
    private Double maxRank = 0.0;
	
	public Corpus(MongoDatabase db, Options opt) {
    	if(opt.cacheSize >= 0){    		
    		this.cache = new LruCache<Integer,CorpusDocument>(opt.cacheSize);
    	}else
    		this.cache = new LruCache<Integer,CorpusDocument>();

    	this.db = db; 
    	this.size = 0L;
	}
	
    /** Generates a new document identifier as an integer. */
    public CorpusDocument getDocument() {
    	CorpusDocument doc = new CorpusDocument();
    	doc.did = this.lastDocID++;
    	return doc;
    }
    
    public CorpusDocument getDocument(Integer did){
    	if(this.cache.containsKey(did))
    		return this.cache.get(did);
    	
    	MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
    	CorpusDocument doc = col.find(eq("did", did)).first();
    	if(doc == null)
    		return null;
    	
    	this.cache.put(did, doc);
    	return doc;
    }
    
    public void saveDocument(CorpusDocument doc){
    	MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);    	
    	col.insertOne(doc);   	
    	this.cache.put(doc.did, doc);
    	this.size++;
    }
	
    public Long getSize(){
    	if(this.size > 0L)
    		return this.size;
    	
    	MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
    	this.size = col.count();
    	return this.size;    	
    }

	public HashMap<String, String> getDocumentsNames(PostingsList pl){
		HashMap<String, String> docsInfo = new HashMap<>();
		
		MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
		for(PostingsEntry pe : pl ){
			
			CorpusDocument doc = this.getDocument(pe.docID);
			if(doc == null)
				continue;
			
			docsInfo.put(Integer.toString(doc.did), doc.name);
		}
		
		return docsInfo;
	}
  
    public Double idf(PostingsList postings){
    	Long corpusSize = this.getSize();
    	Integer docFrequency = postings.size();
    	
    	Double idf = Math.log10(corpusSize / docFrequency);
    	return idf;
    }
    
    public Integer tf(PostingsEntry pe){
    	return pe.positions.size();
    }

	public void savePageranks(ArrayList<CorpusDocument> docs) {
		MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
		
		for(CorpusDocument doc : docs){
			// find a doc with the same name in the db
			CorpusDocument dbDoc = col.find(eq("name", doc.name)).first();
			if(dbDoc == null){
				System.err.println("Couldnt find doc named ["+doc.name+"] in the db, ignoring it...");
				continue;
			}
			
			dbDoc.rank = doc.rank;
			col.findOneAndReplace(eq("_id", dbDoc.id), dbDoc);
		}
		
	}
	
	public Double getMinRank(){
		if(this.minRank < 1.0)
			return this.minRank;
		
		MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);		
		CorpusDocument doc = col.find().sort(new Document("rank", 1)).first();
		
		this.minRank = doc.rank;
		return this.minRank;
	}
	
	public Double getMaxRank(){
		if(this.maxRank > 0.0)
			return this.maxRank;
		
		MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);		
		CorpusDocument doc = col.find().sort(new Document("rank", -1)).first();
		
		this.maxRank = doc.rank;
		return this.maxRank;
	}
    
}
