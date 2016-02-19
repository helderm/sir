package ir;

import static com.mongodb.client.model.Filters.eq;

import java.util.HashMap;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Corpus {
	private Long size;
	private LruCache<Integer, CorpusDocument> cache;
	private MongoDatabase db;
    private int lastDocID = 0;
	
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
    
    public void saveDocument(CorpusDocument doc){
    	MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);    	
    	col.insertOne(doc);   	
    	this.cache.put(doc.did, doc);
    	this.size++;
    }
	
    public Long getSize(){
    	if(this.size > 0)
    		return this.size;
    	
    	MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
    	this.size = col.count();
    	return this.size;    	
    }

	public HashMap<String, String> getDocumentsNames(PostingsList pl){
		HashMap<String, String> docsInfo = new HashMap<>();
		
		MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
		for(PostingsEntry pe : pl ){
			if(this.cache.containsKey(pe.docID)){
				docsInfo.put(Integer.toString(pe.docID), this.cache.get(pe.docID).name);
				continue;
			}
			
			CorpusDocument doc = col.find(eq("did", pe.docID)).first();
			if(doc == null)
				continue;
			
			docsInfo.put(Integer.toString(doc.did), doc.name);
			this.cache.put(doc.did, doc);
		}
		
		return docsInfo;
	}
	
    public Integer getDocumentLength(Integer did){
    	if(this.cache.containsKey(did))
    		return this.cache.get(did).lenght;
    	
    	MongoCollection<CorpusDocument> col = this.db.getCollection("docs", CorpusDocument.class);
    	
    	CorpusDocument doc = col.find(eq("did", did)).first();
    	this.cache.put(doc.did, doc);
    	return doc.lenght;    	
    }
    
}
