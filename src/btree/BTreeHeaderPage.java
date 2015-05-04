package btree;

import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

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

public class BTreeHeaderPage extends HFPage {

	public BTreeHeaderPage(PageId currentId, PageId rootId, int keyType,
			int keyLength, PageId firstLeafPageId) throws IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		super();
		System.out.println("ATTENTION: "+currentId);
		System.out.println("ATTENTION: "+rootId);
		Page tempPage = new Page();
		SystemDefs.JavabaseBM.pinPage(currentId, tempPage, false);
		init(currentId, tempPage);
		SystemDefs.JavabaseBM.unpinPage(currentId, true);

		byte[] tempInt = new byte[4];
		Convert.setIntValue(rootId.pid, 0, tempInt);
		insertRecord(tempInt);
		tempInt = new byte[4];
		Convert.setIntValue(keyType, 0, tempInt);
		insertRecord(tempInt);
		tempInt = new byte[4];
		Convert.setIntValue(keyLength, 0, tempInt);
		insertRecord(tempInt);
		tempInt = new byte[4];
		Convert.setIntValue(firstLeafPageId.pid, 0, tempInt);
		insertRecord(tempInt);
//		try {
//			System.out.println("ATTENTION: "+getPageIdOfRoot());
//		} catch (InvalidSlotNumberException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	public BTreeHeaderPage(Page current) throws IOException {
		super();
//		setCurPage(currentId);
		openHFpage(current);
	}

	public PageId getPageIdOfRoot() throws InvalidSlotNumberException,
			IOException {
		Tuple rootIdTuple = getRecord(firstRecord());
		int myPID = Convert.getIntValue(0, rootIdTuple.getTupleByteArray());
		PageId pageID = new PageId();
		pageID.pid = myPID;
		return pageID;
	}
	public PageId get_rootId() throws InvalidSlotNumberException,
	IOException {
		return getPageIdOfRoot();
	}
	public short get_keyType() throws IOException, InvalidSlotNumberException {
		return (short) getKeyType();
	}
	public  int get_maxKeySize() throws IOException, InvalidSlotNumberException {
		return getKeyLength();
	}
	public void setPageIdOfRoot(PageId pageIdOfRoot) throws IOException,
			InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		new BTreeHeaderPage(getCurPage(), pageIdOfRoot, get_keyType(), get_maxKeySize(), get_firstLeafPageId());
	}

	public PageId get_firstLeafPageId() throws IOException, InvalidSlotNumberException {
		// TODO Auto-generated method stub
		RID tempRID = firstRecord();
		tempRID = nextRecord(tempRID);
		tempRID = nextRecord(tempRID);
		Tuple rootIdTuple = getRecord(nextRecord(tempRID));
		PageId pid =new PageId();
		pid.pid= Convert.getIntValue(0,
				rootIdTuple.getTupleByteArray());
		return pid;
	}

	public int getKeyType() throws IOException, InvalidSlotNumberException {
		RID tempRID = firstRecord();
		Tuple rootIdTuple = getRecord(nextRecord(tempRID));
		int mySearchKey = Convert.getIntValue(0,
				rootIdTuple.getTupleByteArray());
		return mySearchKey;
	}

	public int getKeyLength() throws IOException, InvalidSlotNumberException {
		RID tempRID = firstRecord();
		tempRID = nextRecord(tempRID);
		Tuple rootIdTuple = getRecord(nextRecord(tempRID));
		int myKeyLength = Convert.getIntValue(0,
				rootIdTuple.getTupleByteArray());
		return myKeyLength;
	}

}