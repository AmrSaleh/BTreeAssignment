package btree;

import java.io.IOException;

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

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;

public class BTIndexPage extends BTSortedPage {

	public BTIndexPage(PageId pageId, int keytype) throws ConstructPageException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		super(pageId, keytype);
		setType(NodeType.INDEX);

	}

	public BTIndexPage(Page arg0, int keytype) throws ConstructPageException, IOException {
		super(arg0, keytype);
		setType(NodeType.INDEX);
	}

	public BTIndexPage(int keyType) throws IOException, ConstructPageException {
		super(keyType);
		setType(NodeType.INDEX);
	}
	
	public KeyDataEntry getCurrent(RID rid) throws InvalidSlotNumberException, IOException, KeyNotMatchException, NodeNotMatchException, ConvertException {
		KeyDataEntry entry;
		if (rid == null) {
			return null;
		}
		entry = BT.getEntryFromBytes(getRecord(rid).getTupleByteArray(), 0, getRecord(rid).getTupleByteArray().length, keyType, getType());
		return entry;
	}
	public RID insertKey(KeyClass key, PageId pageNo) throws InsertRecException {
		KeyDataEntry entry = new KeyDataEntry(key, pageNo);
		return super.insertRecord(entry);
	}

	public PageId getLeftLink() throws IOException {

		return getPrevPage();
	}

	public void setLeftLink(PageId left) throws IOException {

		setPrevPage(left);
	}

	public KeyDataEntry getFirst(RID rid) throws IOException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException {
//		rid=firstRecord();
		if(empty())
		{
			
			rid=null;
			return null;
		}
		
		if (firstRecord()==null) {
			rid=null;
			return null;
		}
//		rid = new RID();
		rid.pageNo = firstRecord().pageNo;
		rid.slotNo = firstRecord().slotNo;
		KeyDataEntry entry;
		entry =BT.getEntryFromBytes(getRecord(rid).getTupleByteArray(), 0, getRecord(rid).getTupleByteArray().length, keyType, getType());

		
		return entry;
	}

	public KeyDataEntry getNext(RID rid) throws IOException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException {
		
//		return BT.getEntryFromBytes(getRecord(rid).getTupleByteArray(), 0, getRecord(rid).getTupleByteArray().length, keyType, getType());
		
		if (rid == null) {
			return null;
		}
		RID tempRID = nextRecord(rid);
		if(tempRID!=null)
		{
			rid.pageNo = tempRID.pageNo;
			rid.slotNo = tempRID.slotNo;
		}
		else
		{
			rid = null;
		}

		KeyDataEntry entry;
		if (rid == null) {
			return null;
		}
		entry = BT.getEntryFromBytes(getRecord(rid).getTupleByteArray(), 0,
				getRecord(rid).getTupleByteArray().length, keyType, getType());
		return entry;
	}
}
