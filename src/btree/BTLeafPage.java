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
import heap.InvalidSlotNumberException;

public class BTLeafPage extends BTSortedPage {

	public BTLeafPage(PageId pageId, int keytype)
			throws ConstructPageException, IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException {
		super(pageId, keytype);
		setType(NodeType.LEAF);

	}

	public BTLeafPage(Page arg0, int keytype) throws ConstructPageException,
			IOException {
		super(arg0, keytype);
		setType(NodeType.LEAF);
		// TODO Auto-generated constructor stub

	}

	public BTLeafPage(int keyType) throws IOException, ConstructPageException {
		super(keyType);
		setType(NodeType.LEAF);
	}

	public boolean delEntry(KeyDataEntry dEntry)
			throws InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, IOException, DeleteRecException {
		if (dEntry == null) {
			return false;
		}
		RID rid = new RID();
		if (empty()) {
			return false;
		}
		if (getFirst(rid).equals(dEntry)) {

			deleteSortedRecord(rid);
//			System.out.println("el page feeha we masa7to");
			return true;
		}
		KeyDataEntry tempPair = getNext(rid);
		while (tempPair != null) {
//			System.out.println("infinite loop");
			if (getCurrent(rid).equals(dEntry)) {
				deleteSortedRecord(rid);
				return true;
			}
			tempPair = getNext(rid);
		}
		return false;
	}

	
	public boolean delEntryWithoutCompact(KeyDataEntry dEntry)
			throws InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, IOException, DeleteRecException {
		if (dEntry == null) {
			return false;
		}
		RID rid = new RID();
		if (empty()) {
			return false;
		}
		if (getFirst(rid).equals(dEntry)) {

			deleteRecord(rid);
			System.out.println("el page feeha we masa7to");
			return true;
		}
		KeyDataEntry tempPair = getNext(rid);
		while (tempPair != null) {
			System.out.println("infinite loop");
			if (getCurrent(rid).equals(dEntry)) {
				deleteSortedRecord(rid);
				return true;
			}
			tempPair = getNext(rid);
		}
		return false;
	}
	
	// delete a data entry in the leaf page.
	public KeyDataEntry getCurrent(RID rid) throws InvalidSlotNumberException,
			IOException, KeyNotMatchException, NodeNotMatchException,
			ConvertException {
		KeyDataEntry entry;
		if (rid == null) {
			return null;
		}
		entry = BT.getEntryFromBytes(getRecord(rid).getTupleByteArray(), 0,
				getRecord(rid).getTupleByteArray().length, keyType, getType());
		return entry;
	}

	// getCurrent returns the current record in the iteration; it is like
	// getNext except it does not advance the iterator.
	public KeyDataEntry getFirst(RID rid) throws IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException {
		if (empty()) {
			return null;
		}
		rid.pageNo = firstRecord().pageNo;
		rid.slotNo = firstRecord().slotNo;
		return getCurrent(rid);
	}

	// Iterators.
	public KeyDataEntry getNext(RID rid) throws IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException {
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
		return getCurrent(rid);
	}

	// Iterators.
	public RID insertRecord(KeyClass key, RID dataRid)
			throws InsertRecException, InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			IOException, DeleteRecException {
		KeyDataEntry entry = new KeyDataEntry(key, dataRid);
		delEntry(entry);
		return super.insertRecord(entry);
	}
	// insertRecord.
}
