package btree;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;

import java.io.IOException;
import java.util.ArrayList;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;

public class BTFileScan extends IndexFileScan implements GlobalConst {

	private KeyClass lowKey,highKey;
	private BTreeFile myBTree;
	private boolean firstDone = false;
	private PageId scanPID;
	private RID currRID=new RID();
	public BTFileScan() {
		// TODO Auto-generated constructor stub
	}
	
public BTFileScan(BTreeFile btree, KeyClass lowkey, KeyClass hikey) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException, HashEntryNotFoundException, InvalidSlotNumberException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException {
		// TODO Auto-generated constructor stub
		scanPID = new PageId();
		myBTree=btree;
		lowKey=lowkey;
		highKey=hikey;
		if(lowKey == null)
		{
			scanPID.pid = myBTree.getHeaderPage().get_firstLeafPageId().pid;
			Page tempPage = new Page();
		    SystemDefs.JavabaseBM.pinPage(scanPID, tempPage, false);
		    BTLeafPage leaf = new BTLeafPage(tempPage,btree.getHeaderPage().get_keyType());
		    RID rid = new RID();
			leaf.getFirst(rid);
			currRID.copyRid(rid);
			SystemDefs.JavabaseBM.unpinPage(scanPID, true);
			return;
		}
	    ArrayList<Integer> ids =myBTree.search(lowKey);
	    for (int i = 0; i < ids.size(); i++) {
			System.out.println("id("+i+") ==> "+ids.get(i));
		}
	    PageId pageID = new PageId();
	    pageID.pid=ids.get(ids.size()-1);
	    System.out.println("search scan pid "+ pageID.pid);
//	    SystemDefs.JavabaseBM.unpinPage(pageID, true); // because of search
	    Page tempPage = new Page();
	    SystemDefs.JavabaseBM.pinPage(pageID, tempPage, false);
	    System.out.println("search scan pid "+ pageID.pid);
	    BTLeafPage leaf = new BTLeafPage(tempPage,btree.getHeaderPage().get_keyType());
	    scanPID.pid = leaf.getCurPage().pid;
	    RID rid = new RID();
	    KeyDataEntry tempPair = leaf.getFirst(rid);
	    while( tempPair!=null && BT.keyCompare(tempPair.key,lowKey)<0 )
	    {
	        tempPair = leaf.getNext(rid);
	    }
	    if(tempPair==null)
	    {
	        if(leaf.getNextPage().pid==-1)
	        {
	            scanPID = null;
	            currRID=null;
	            SystemDefs.JavabaseBM.unpinPage(pageID, true);
	            return;
	        }
	        else
	        {
    	        scanPID.pid = leaf.getNextPage().pid;
    	        SystemDefs.JavabaseBM.pinPage(scanPID, tempPage, false);
    	        leaf = new BTLeafPage(tempPage,btree.getHeaderPage().get_keyType());
    	        tempPair = leaf.getFirst(rid);
    	        SystemDefs.JavabaseBM.unpinPage(scanPID, true);
	        }
	    }
	    currRID.copyRid(rid);
	    SystemDefs.JavabaseBM.unpinPage(pageID, true);
	}

	public KeyDataEntry get_next() throws InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, HashEntryNotFoundException, IOException {
		// TODO Auto-generated method stub
		
	if(scanPID==null){
	    return null;
	}
	if(!firstDone){
	    firstDone=!firstDone;
	    
	    Page tempPage = new Page();
	    SystemDefs.JavabaseBM.pinPage(scanPID, tempPage, false);
	    BTLeafPage leaf = new BTLeafPage(tempPage,myBTree.getHeaderPage().get_keyType());
	    KeyDataEntry entry = leaf.getCurrent(currRID);
	    SystemDefs.JavabaseBM.unpinPage(scanPID, true);
	    return entry;
	}
	
	    Page tempPage = new Page();
	    SystemDefs.JavabaseBM.pinPage(scanPID, tempPage, false);
	    BTLeafPage leaf = new BTLeafPage(tempPage,myBTree.getHeaderPage().get_keyType());
	    KeyDataEntry entry = leaf.getNext(currRID);
	    if (entry==null) {
			if (leaf.getNextPage().pid==-1) {
				scanPID=null;
				return null;
						
			}else{
				scanPID.pid=leaf.getNextPage().pid;

				tempPage = new Page();
			    SystemDefs.JavabaseBM.pinPage(scanPID, tempPage, false);
			    leaf = new BTLeafPage(tempPage,myBTree.getHeaderPage().get_keyType());
			    entry = leaf.getFirst(currRID);
			    SystemDefs.JavabaseBM.unpinPage(scanPID, true);
			    if (entry==null) {
			    	scanPID=null;
					return null;
				}
				return entry;
			}
		}
	    
	    if( highKey!=null && BT.keyCompare(entry.key,highKey)>0){
	        
	        SystemDefs.JavabaseBM.unpinPage(scanPID, true);
	        scanPID=null;
	        return null;
	    }
	    SystemDefs.JavabaseBM.unpinPage(scanPID, true);
	    return entry;
	}

	public void delete_current() throws InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, HashEntryNotFoundException, DeleteRecException {
		// TODO Auto-generated method stub
		if(currRID==null || scanPID==null)
		{
			return;
		}
		Page tempPage = new Page();
	    SystemDefs.JavabaseBM.pinPage(scanPID, tempPage, false);
	    BTLeafPage leaf = new BTLeafPage(tempPage,myBTree.getHeaderPage().get_keyType());
		KeyDataEntry keyToDelete = leaf.getCurrent(currRID);
		leaf.delEntry(keyToDelete);
		SystemDefs.JavabaseBM.unpinPage(scanPID, true);
	}

	@Override
	public int keysize() {
		// TODO Auto-generated method stub
		return myBTree.getKeySize();
	}

}
