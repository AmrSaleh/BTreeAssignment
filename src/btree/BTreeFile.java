package btree;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.SpaceNotAvailableException;
import java.io.IOException;
import java.util.ArrayList;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidBufferException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BTreeFile extends IndexFile implements GlobalConst {
	private BTreeHeaderPage headerPage;

	private int keySize, keyType;
	private String fileEntryName;

	public BTreeFile(String string, int typeOfKey, int i, int j) throws ConstructPageException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException, DiskMgrException, IOException, FileNameTooLongException, InvalidPageNumberException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, FileIOException, InvalidSlotNumberException {
		// TODO Auto-generated constructor stub
		HFPage hfpage = new HFPage();
		fileEntryName = string;
		Page tempPage = new Page();
		if (string == null || string.equals("") || typeOfKey > 1) {
			// produce a temp file here
		} else if (exist(string)) // file already exists
		{
			PageId headerPID = SystemDefs.JavabaseDB.get_file_entry(string);
			SystemDefs.JavabaseBM.pinPage(headerPID, tempPage, false);
			headerPage = new BTreeHeaderPage(tempPage);
			keySize = headerPage.getKeyLength();
			keyType = headerPage.get_keyType();
			// open the file here
		} else {
			keySize = i;
			keyType = typeOfKey;
			PageId rootPageID = SystemDefs.JavabaseBM.newPage(tempPage, 1);
			SystemDefs.JavabaseBM.pinPage(rootPageID, tempPage, false);
			hfpage.init(rootPageID, tempPage);
			BTLeafPage root = new BTLeafPage(tempPage, keyType);
			// System.out.println("LeafPageID: "+ root.getCurPage());
			// System.out.println(root.available_space());
			PageId headerPID = SystemDefs.JavabaseBM.newPage(tempPage, 1);
			// System.out.println("Attention: "+headerPID.pid);
			SystemDefs.JavabaseDB.add_file_entry(string, headerPID);
			headerPage = new BTreeHeaderPage(headerPID, root.getCurPage(), keyType, i, root.getCurPage());
			headerPage.setNextPage(root.getCurPage());
			SystemDefs.JavabaseBM.unpinPage(rootPageID, true);
			SystemDefs.JavabaseBM.unpinPage(rootPageID, true);
		}
	}

	public BTreeFile(String string) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InvalidSlotNumberException, HashEntryNotFoundException, FileEntryNotFoundException {
		fileEntryName = string;
		Page tempPage = new Page();
		if (string == null || string.equals("")) {
			// produce a temp file here
		} else if (exist(string)) // file already exists
		{
			PageId headerPID = SystemDefs.JavabaseDB.get_file_entry(string);
			SystemDefs.JavabaseBM.pinPage(headerPID, tempPage, false);
			headerPage = new BTreeHeaderPage(tempPage);
			keySize = headerPage.getKeyLength();
			keyType = headerPage.get_keyType();
			// open the file here
		} else {
			throw new FileEntryNotFoundException(new Exception(), "File entry not found");
		}
	}

	private boolean exist(String string) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException { // check
		// method
		// stub
		if (SystemDefs.JavabaseDB.get_file_entry(string) != null) {
			return true;
		}
		return false;
	}

	public void close() throws ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, IOException {
		// TODO Auto-generated method stub

		try {
			SystemDefs.JavabaseBM.unpinPage(headerPage.getCurPage(), true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return;
		}

	}

	public ArrayList<Integer> search(KeyClass key) throws InvalidSlotNumberException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, HashEntryNotFoundException {
		boolean check = false;
		ArrayList<Integer> pids = new ArrayList<Integer>();
		PageId tempPageId = new PageId();
		tempPageId.pid = headerPage.get_rootId().pid;
		// System.out.println("Search method: PageID of root: "+ tempPageId);
		// System.out.println("ATTENTION: "+ tempPageId.pid);
		Page tempPage = new Page();

		SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
		HFPage tempHfPage = new HFPage();
		// tempHfPage.openHFpage(tempPageId);
		tempHfPage.openHFpage(tempPage);

		// pids.add(tempPageId.pid);

		BTIndexPage tempIndexPage;
		RID tempRID = new RID();
		// RID tempRID2 = new RID();
		RID previousRID = new RID();
		KeyDataEntry tempPair, prevPair;
		SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
		if (tempHfPage.getType() == NodeType.LEAF) {
			pids.add(tempPageId.pid);
			check = true;
		}
		while (tempHfPage.getType() != NodeType.LEAF) {
			SystemDefs.JavabaseBM.pinPage(tempHfPage.getCurPage(), tempPage, false);
			pids.add(tempHfPage.getCurPage().pid);
			tempIndexPage = new BTIndexPage(tempPage, keyType);
			SystemDefs.JavabaseBM.unpinPage(tempHfPage.getCurPage(), false);

			tempRID = new RID();

			tempPair = tempIndexPage.getFirst(tempRID);
			// if (tempPair == null) {
			// // SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
			// // SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			// // return pids;
			// PageId idfortest =new PageId();
			// idfortest=tempIndexPage.getLeftLink();
			// SystemDefs.JavabaseBM.pinPage(tempIndexPage.getLeftLink(),
			// tempPage, false);
			// tempHfPage.openHFpage(tempPage);
			// SystemDefs.JavabaseBM.unpinPage(idfortest, false);
			//
			// continue;
			//
			// }
			KeyClass tempKey = tempPair.key;
			if (BT.keyCompare(key, tempKey) < 0) {
				SystemDefs.JavabaseBM.pinPage(tempIndexPage.getLeftLink(), tempPage, false);

				tempHfPage.openHFpage(tempPage);
				SystemDefs.JavabaseBM.unpinPage(tempIndexPage.getLeftLink(), false);
				continue;
			} else {

				previousRID.pageNo.pid = tempRID.pageNo.pid;
				previousRID.slotNo = tempRID.slotNo;
				prevPair = new KeyDataEntry(tempPair.key, tempPair.data);

				RID testingRid = tempIndexPage.nextRecord(previousRID);
				if (testingRid == null) {
					tempRID = null;
				} else {
					tempRID.copyRid(testingRid);
				}

				System.out.println("tempPair  key " + tempPair.key + "  data  " + tempPair.data);
				tempPair = tempIndexPage.getNext(previousRID);
				if (tempPair != null) {
					System.out.println("tempPair  key " + tempPair.key + "  data  " + tempPair.data);
					tempKey = tempPair.key;
				}

				while (tempRID != null) {
					if (BT.keyCompare(key, tempKey) < 0) {
						SystemDefs.JavabaseBM.pinPage(((IndexData) (prevPair.data)).getData(), tempPage, false);
						tempHfPage.openHFpage(tempPage);
						SystemDefs.JavabaseBM.unpinPage(((IndexData) (prevPair.data)).getData(), false);
						break;
					} else {
						previousRID.pageNo.pid = tempRID.pageNo.pid;
						previousRID.slotNo = tempRID.slotNo;
						prevPair = new KeyDataEntry(tempPair.key, tempPair.data);
						tempRID = tempIndexPage.nextRecord(previousRID);
						tempPair = tempIndexPage.getNext(previousRID);
						if (tempPair == null) {
							// SystemDefs.JavabaseBM.unpinPage(tempPageId,
							// false);
							// SystemDefs.JavabaseBM.pinPage(tempPageId,
							// tempPage, false);
							// return pids;
							SystemDefs.JavabaseBM.pinPage(((IndexData) (prevPair.data)).getData(), tempPage, false);
							tempHfPage.openHFpage(tempPage);
							SystemDefs.JavabaseBM.unpinPage(((IndexData) (prevPair.data)).getData(), false);
							tempRID = new RID();
							break;
						}
						tempKey = tempPair.key;
					}
				}
				if (tempRID == null) {
					SystemDefs.JavabaseBM.pinPage(((IndexData) (prevPair.data)).getData(), tempPage, false);
					tempHfPage.openHFpage(tempPage);
					SystemDefs.JavabaseBM.unpinPage(((IndexData) (prevPair.data)).getData(), false);
					tempRID = new RID();

				}
			}
			// pids.add(tempHfPage.getCurPage().pid);
			// tempPageId.pid = pids.get(pids.size() - 2);
			// SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
			// SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			// tempPageId.pid = pids.get(pids.size() - 1);

		}
		if (check == false) {
			pids.add(tempHfPage.getCurPage().pid);
		}

		for (int i = 0; i < pids.size(); i++) {
			System.out.println("id(" + i + ") => " + pids.get(i));
		}
		return pids;

	}

	// private int[] doubleArraySize(int[] array) {
	// int[] doubleSizedArray = new int[array.length * 2];
	// for (int i = 0; i < array.length; i++) {
	// doubleSizedArray[i] = array[i];
	// }
	// return doubleSizedArray;
	// }

	public BTFileScan new_scan(KeyClass lowkey, KeyClass hikey) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException, InvalidSlotNumberException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, IOException, SpaceNotAvailableException {
		// TODO Auto-generated method stub
		if (hikey != null && lowkey != null && BT.keyCompare(hikey, lowkey) < 0) {
			throw new SpaceNotAvailableException(new Exception(), "Page size exceeded");
		}
		BTFileScan scan = new BTFileScan(this, lowkey, hikey);
		return scan;
	}

	public void destroyFile() throws ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, IOException, FileEntryNotFoundException, FileIOException, InvalidPageNumberException, DiskMgrException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException, InvalidBufferException {
		// TODO Auto-generated method stub
		ArrayList<PageId> pageIds = new ArrayList<PageId>();
		pageIds.add(headerPage.get_rootId());
		while (pageIds.size() > 0) {
			ArrayList<PageId> childrenPIDs = getChildrenIDs(pageIds.get(0));
			for (int i = 0; i < childrenPIDs.size(); i++) {
				pageIds.add(childrenPIDs.get(i));
			}
			SystemDefs.JavabaseBM.freePage(pageIds.get(0));
			pageIds.remove(0);
		}

		close();
		SystemDefs.JavabaseBM.freePage(headerPage.getCurPage());
		SystemDefs.JavabaseDB.delete_file_entry(fileEntryName);

	}

	private ArrayList<PageId> getChildrenIDs(PageId pageId) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException, HashEntryNotFoundException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, InvalidSlotNumberException {
		ArrayList<PageId> childPIDs = new ArrayList<PageId>();
		Page tempPage = new Page();
		SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
		BTSortedPage sortedPage = new BTSortedPage(tempPage, keyType);
		if (sortedPage.getType() == NodeType.LEAF) {
			SystemDefs.JavabaseBM.unpinPage(pageId, true);
			return childPIDs;
		}
		BTIndexPage indexPage = new BTIndexPage(tempPage, keyType);
		RID rid = new RID();
		childPIDs.add(indexPage.getLeftLink());
		KeyDataEntry dataEntry = indexPage.getFirst(rid);
		while (dataEntry != null) {
			childPIDs.add(((IndexData) dataEntry.data).getData());
			indexPage.getNext(rid);
		}
		SystemDefs.JavabaseBM.unpinPage(pageId, true);
		return childPIDs;
	}

	// public void destroyFile() throws ReplacerException,
	// PageUnpinnedException, HashEntryNotFoundException,
	// InvalidFrameNumberException, IOException, FileEntryNotFoundException,
	// FileIOException, InvalidPageNumberException, DiskMgrException,
	// HashOperationException, PageNotReadException,
	// BufferPoolExceededException, PagePinnedException, BufMgrException,
	// ConstructPageException, KeyNotMatchException, NodeNotMatchException,
	// ConvertException, InvalidSlotNumberException, InvalidBufferException {
	// // TODO Auto-generated method stub
	// ArrayList<PageId> pageIds = new ArrayList<PageId>();
	// pageIds= getChildrenIDs(headerPage.getPageIdOfRoot());
	//
	// for (int i = 0; i < pageIds.size(); i++) {
	// SystemDefs.JavabaseBM.freePage(pageIds.get(i));
	// }
	//
	//
	// close();
	// SystemDefs.JavabaseBM.freePage(headerPage.getCurPage());
	//
	// SystemDefs.JavabaseDB.delete_file_entry(fileEntryName);
	//
	// }
	//
	// private ArrayList<PageId> getChildrenIDs(PageId pageId) throws
	// ReplacerException, HashOperationException, PageUnpinnedException,
	// InvalidFrameNumberException, PageNotReadException,
	// BufferPoolExceededException, PagePinnedException, BufMgrException,
	// IOException, HashEntryNotFoundException, ConstructPageException,
	// KeyNotMatchException, NodeNotMatchException, ConvertException,
	// InvalidSlotNumberException{
	// ArrayList<PageId> resultIds = new ArrayList<PageId>();
	// ArrayList<PageId> secondIds = new ArrayList<PageId>();
	// Page tempPage = new Page();
	// SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
	// BTSortedPage sortedPage = new BTSortedPage(tempPage,keyType);
	//
	// resultIds.add(pageId);
	// if (sortedPage.getType()==NodeType.LEAF) {
	// SystemDefs.JavabaseBM.unpinPage(pageId, true);
	// return resultIds;
	// }
	// BTIndexPage indexPage=new BTIndexPage(tempPage,keyType);
	// RID rid = new RID();
	// KeyDataEntry dataEntry =indexPage.getFirst(rid);
	// secondIds = (ArrayList<PageId>)
	// getChildrenIDs(indexPage.getLeftLink()).clone();
	// for (int i = 0; i < secondIds.size(); i++) {
	// resultIds.add(secondIds.get(i));
	// }
	// while (dataEntry!=null) {
	// // resultIds.add(((IndexData) dataEntry.data).getData());
	// secondIds = (ArrayList<PageId>) getChildrenIDs(((IndexData)
	// dataEntry.data).getData()).clone();
	//
	// for (int i = 0; i < secondIds.size(); i++) {
	// resultIds.add(secondIds.get(i));
	// }
	//
	// indexPage.getNext(rid);
	//
	//
	// }
	//
	//
	//
	// SystemDefs.JavabaseBM.unpinPage(pageId, true);
	// return resultIds;
	// }

	public void traceFilename(String string) {
		// TODO Auto-generated method stub

	}

	public void insert(KeyClass key, RID rid) throws InsertRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, IOException, ConstructPageException, InvalidSlotNumberException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, KeyNotMatchException, NodeNotMatchException, ConvertException, DeleteRecException, SpaceNotAvailableException {
		// TODO Auto-generated method stub
		System.out.println("start unpinned    " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
		if (BT.getKeyLength(key) > keySize) {
			throw new SpaceNotAvailableException(new Exception(), "Key size exceeded");
		}
		HFPage hfPage = new HFPage();
		Page hfTempPage = new Page();
		BTLeafPage tempLeafPage = new BTLeafPage(keyType);
		// hfPage.init(tempLeafPage.getCurPage(), hfTempPage);
		tempLeafPage.insertRecord(key, rid);
		int entrySize = tempLeafPage.getRecord(tempLeafPage.firstRecord()).getLength();
		if (entrySize > global.GlobalConst.MINIBASE_PAGESIZE) {
			throw new SpaceNotAvailableException(new Exception(), "Page size exceeded");
			// return;
		}
		// System.out.println("Entry Size: " + entrySize);
		ArrayList<Integer> resultPagesPIDs = search(key);

		PageId leafPageID = new PageId();
		// System.out.println("First insert: array length: " +
		// resultPagesPIDs.size());
		leafPageID.pid = resultPagesPIDs.get(resultPagesPIDs.size() - 1);
		// System.out.println("LeafPageID: " + leafPageID);
		Page tempPage = new Page();
		SystemDefs.JavabaseBM.pinPage(leafPageID, tempPage, false);
		BTLeafPage resultPage = new BTLeafPage(tempPage, keyType);
		// System.out.println("SPACE: " + resultPage.available_space());
		KeyDataEntry newPair = new KeyDataEntry(key, rid);

		if (resultPage.available_space() < entrySize) {
			split(resultPagesPIDs, newPair);
		} else {
			resultPage.insertRecord(key, rid);
		}

		SystemDefs.JavabaseBM.unpinPage(resultPage.getCurPage(), true);
		SystemDefs.JavabaseBM.unpinPage(tempLeafPage.getCurPage(), true);
		// SystemDefs.JavabaseBM.unpinPage(leafPageID, true);
		System.out.println("End unpinned    " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
	}

	public boolean Delete(KeyClass key, RID rid) throws InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, HashEntryNotFoundException, IOException, DeleteRecException {
		// TODO Auto-generated method stub
		// try {

		ArrayList<Integer> resultPagesPIDs = search(key);
		PageId leafPageID = new PageId();
		leafPageID.pid = resultPagesPIDs.get(resultPagesPIDs.size() - 1);
		Page tempPage = new Page();
		SystemDefs.JavabaseBM.pinPage(leafPageID, tempPage, false);
		BTLeafPage resultPage = new BTLeafPage(tempPage, keyType);
		KeyDataEntry tempPair = new KeyDataEntry(key, rid);
		boolean deleted = resultPage.delEntry(tempPair);
		SystemDefs.JavabaseBM.unpinPage(leafPageID, true);
		// SystemDefs.JavabaseBM.unpinPage(leafPageID, true);
		return deleted;
		// } catch (Exception e) {
		// return false;
		// }

	}

	public BTreeHeaderPage getHeaderPage() {
		// TODO Auto-generated method stub
		return headerPage;
	}

	public void split(ArrayList<Integer> resultPagesPIDs, KeyDataEntry newPair) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException, ConstructPageException, HashEntryNotFoundException, InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, InsertRecException, DeleteRecException {

		ArrayList<KeyDataEntry> ridsToDelete = new ArrayList<KeyDataEntry>();
		KeyDataEntry testPair;
		HFPage hfPage = new HFPage();
		Page hfTempPage = new Page();
		PageId leafPageID = new PageId();
		leafPageID.pid = resultPagesPIDs.get(resultPagesPIDs.size() - 1);
		Page tempPage = new Page();
		SystemDefs.JavabaseBM.pinPage(leafPageID, tempPage, false);
		BTLeafPage resultPage = new BTLeafPage(tempPage, keyType);

		BTLeafPage newLeafPage = new BTLeafPage(keyType);
		// hfPage.init(newLeafPage.getCurPage(), hfTempPage);
		int halfPageSize = global.GlobalConst.MINIBASE_PAGESIZE / 2;
		int comulativeSize = 0;
		RID rid = new RID();
		// RID middleValueRID = new RID();
		resultPage.getFirst(rid);
		comulativeSize += resultPage.getRecord(rid).getLength();
		int num = 0;
		while (comulativeSize < halfPageSize) {
			resultPage.getNext(rid);
			comulativeSize += resultPage.getRecord(rid).getLength();
			num++;
		}
		KeyDataEntry tempPair;
		tempPair = resultPage.getNext(rid);
		// middleValueRID.copyRid(rid);
		KeyDataEntry middlePair = resultPage.getCurrent(rid);
		testPair = resultPage.getCurrent(rid);
		newLeafPage.insertRecord(middlePair.key, ((LeafData) middlePair.data).getData());
		// KeyDataEntry tempPair;
		// exception if middle key = null TODO
		// num=0;
		while (tempPair != null) {

			// System.out.println("move element "+num);
			// System.out.println("move element key"+tempPair.key);
			// num++;

			System.out.println("loop");
			tempPair.key = resultPage.getCurrent(rid).key;
			tempPair.data = resultPage.getCurrent(rid).data;
			newLeafPage.insertRecord(tempPair.key, ((LeafData) tempPair.data).getData());
			tempPair = resultPage.getNext(rid);
			// resultPage.delEntry(tempPair);

		}
		// //////////////////////// deleting
		tempPair = new KeyDataEntry(resultPage.getFirst(rid).key, resultPage.getFirst(rid).data);
		// tempPair.key = resultPage.getFirst(rid).key;
		// tempPair.data = resultPage.getFirst(rid).data;
		int tempnum = 0;

		while (tempPair != null) {

			if (tempnum > num) {
				// resultPage.delEntry(tempPair);
				ridsToDelete.add(tempPair);
				// tempPair = resultPage.getCurrent(rid);
				// resultPage.delEntryWithoutCompact(tempPair);
			}
			// tempPair.key = resultPage.getCurrent(rid).key;
			// tempPair.data = resultPage.getCurrent(rid).data;

			tempPair = resultPage.getNext(rid);

			tempnum++;

		}

		for (int i = 0; i < ridsToDelete.size(); i++) {
			resultPage.delEntry(ridsToDelete.get(i));
		}
		// ////////////////////////////////
		if (BT.keyCompare(newPair.key, middlePair.key) < 0) {
			resultPage.insertRecord(newPair.key, ((LeafData) newPair.data).getData());
		} else {
			newLeafPage.insertRecord(newPair.key, ((LeafData) newPair.data).getData());
		}

		resultPage.setNextPage(newLeafPage.getCurPage());
		// next step split index pages
		tempPair = new KeyDataEntry(middlePair.key, newLeafPage.getCurPage());
		System.out.println("the pair of middle  " + tempPair.key + "  data  " + tempPair.data);
		BTIndexPage tempIndexPage = new BTIndexPage(keyType);
		// hfPage.init(tempIndexPage.getCurPage(), hfTempPage);
		tempIndexPage.insertKey(middlePair.key, newLeafPage.getCurPage());
		System.out.println("rid page " + rid.pageNo + "  slot " + rid.slotNo);
		tempIndexPage.getFirst(rid);
		System.out.println("rid page " + rid.pageNo + "  slot " + rid.slotNo);
		int copyUpEntrySize = tempIndexPage.getRecord(rid).getLength();
		SystemDefs.JavabaseBM.unpinPage(tempIndexPage.getCurPage(), true);
		
		
		if (resultPagesPIDs.size() > 1) {

			PageId tempPageID = new PageId();
			tempPageID.pid = resultPagesPIDs.get(resultPagesPIDs.size() - 2);
			;
			// Page tempPage = new Page();
			SystemDefs.JavabaseBM.pinPage(tempPageID, tempPage, false);
			tempIndexPage = new BTIndexPage(tempPage, keyType);

			if (copyUpEntrySize > tempIndexPage.available_space()) {

				SystemDefs.JavabaseBM.unpinPage(tempPageID, true);
				for (int i = resultPagesPIDs.size() - 2; i > -1; i--) {

					PageId tempID = new PageId();
					tempID.pid = resultPagesPIDs.get(i);
					;
					// Page tempPage = new Page();
					SystemDefs.JavabaseBM.pinPage(tempID, tempPage, false);
					tempIndexPage = new BTIndexPage(tempPage, keyType);
					SystemDefs.JavabaseBM.unpinPage(tempPageID, true);
					if (copyUpEntrySize > tempIndexPage.available_space()) {
						comulativeSize = 0;
						tempPageID = new PageId();
						tempPageID.pid = resultPagesPIDs.get(i);
						;
						SystemDefs.JavabaseBM.pinPage(tempPageID, tempPage, false);
						tempIndexPage = new BTIndexPage(tempPage, keyType);
						tempIndexPage.getFirst(rid);
						comulativeSize += tempIndexPage.getRecord(rid).getLength();
						num = 0;
						ArrayList<RID> ridsToDelete2 = new ArrayList<RID>();
						while (comulativeSize < halfPageSize) {
							tempIndexPage.getNext(rid);
							comulativeSize += tempIndexPage.getRecord(rid).getLength();
							num++;
						}
						tempIndexPage.getNext(rid);
						BTIndexPage newIndexPage = new BTIndexPage(keyType);
						// hfPage.init(newIndexPage.getCurPage(), hfTempPage);
						KeyDataEntry middleIndexPair = tempIndexPage.getCurrent(rid);
						
						ridsToDelete2.add(rid);
						
						testPair = tempIndexPage.getNext(rid);
						KeyDataEntry indexShiftPair = new KeyDataEntry(testPair.key, testPair.data);						// tempIndexPage.deleteSortedRecord(rid);

						
						while (testPair != null && rid != null) {
							indexShiftPair.key = tempIndexPage.getCurrent(rid).key;
							indexShiftPair.data = tempIndexPage.getCurrent(rid).data;
							// testPair = tempIndexPage.getCurrent(rid);
							newIndexPage.insertKey(indexShiftPair.key, ((IndexData) indexShiftPair.data).getData());

							testPair = tempIndexPage.getNext(rid);
							// tempIndexPage.deleteSortedRecord(rid);
							System.out.println("ana henaaaaaaaaa");
						}
						// //////////////////////// deleting
						RID testRid2 = new RID();
						KeyDataEntry testPair2 = new KeyDataEntry(tempIndexPage.getFirst(testRid2).key, tempIndexPage.getFirst(testRid2).data);
						// tempPair.key = resultPage.getFirst(rid).key;
						// tempPair.data = resultPage.getFirst(rid).data;
						tempnum = 0;

						while (testPair2 != null) {

							if (tempnum > num) {
								// resultPage.delEntry(tempPair);
								ridsToDelete2.add(testRid2);
								// tempPair = resultPage.getCurrent(rid);
								// resultPage.delEntryWithoutCompact(tempPair);
							}
							// tempPair.key = resultPage.getCurrent(rid).key;
							// tempPair.data = resultPage.getCurrent(rid).data;

							testPair2 = tempIndexPage.getNext(testRid2);

							tempnum++;

						}

						for (int i1 = 0; i1 < ridsToDelete.size(); i1++) {
							tempIndexPage.deleteSortedRecord(ridsToDelete2.get(i1));
						}
						// ////////////////////////////////
						if (BT.keyCompare(tempPair.key, middleIndexPair.key) < 0) {
							tempIndexPage.insertKey(tempPair.key, ((IndexData) tempPair.data).getData());
						} else {
							newIndexPage.insertKey(tempPair.key, ((IndexData) tempPair.data).getData());
						}
						PageId newPageID = new PageId();
						newPageID.copyPageId(((IndexData) middleIndexPair.data).getData());
						newIndexPage.setLeftLink(newPageID);
						middleIndexPair = new KeyDataEntry(middleIndexPair.key, newIndexPage.getCurPage());

						SystemDefs.JavabaseBM.unpinPage(newIndexPage.getCurPage(), true);
						SystemDefs.JavabaseBM.unpinPage(tempPageID, true);

						if (i > 0) {
							tempPageID = new PageId();
							tempPageID.pid = resultPagesPIDs.get(i - 1);

							SystemDefs.JavabaseBM.pinPage(tempPageID, tempPage, false);
							tempIndexPage = new BTIndexPage(tempPage, keyType);

							BTIndexPage middleIndexPage = new BTIndexPage(keyType);
							// hfPage.init(middleIndexPage.getCurPage(),
							// hfTempPage);
							middleIndexPage.insertKey(middleIndexPair.key, ((IndexData) (middleIndexPair.data)).getData());
							middleIndexPage.getFirst(rid);
							copyUpEntrySize = middleIndexPage.getRecord(rid).getLength();

							tempPair = middleIndexPair;

							SystemDefs.JavabaseBM.unpinPage(tempPageID, true);
							SystemDefs.JavabaseBM.unpinPage(middleIndexPage.getCurPage(), true);
						} else {
							BTIndexPage middleIndexPage = new BTIndexPage(keyType);
							// hfPage.init(middleIndexPage.getCurPage(),
							// hfTempPage);
							middleIndexPage.insertKey(middleIndexPair.key, ((IndexData) (middleIndexPair.data)).getData());

							middleIndexPage.setLeftLink(tempIndexPage.getCurPage());
							headerPage.setPageIdOfRoot(middleIndexPage.getCurPage());
							SystemDefs.JavabaseBM.unpinPage(middleIndexPage.getCurPage(), true);
							break;
						}

					} else {

						tempIndexPage.insertKey(tempPair.key, ((IndexData) tempPair.data).getData());
						SystemDefs.JavabaseBM.unpinPage(tempPageID, true);
						break;
					}
				}
			} else {

				tempIndexPage.insertKey(tempPair.key, ((IndexData) tempPair.data).getData());
				SystemDefs.JavabaseBM.unpinPage(tempPageID, true);
			}
		} else {
			System.out.println("root index creadted");
			SystemDefs.JavabaseBM.pinPage(tempIndexPage.getCurPage(), tempPage, false);
			tempIndexPage.setLeftLink(resultPage.getCurPage());
			headerPage.setPageIdOfRoot(tempIndexPage.getCurPage());
			SystemDefs.JavabaseBM.unpinPage(tempIndexPage.getCurPage(), true);
		}
		SystemDefs.JavabaseBM.unpinPage(newLeafPage.getCurPage(), true);
		SystemDefs.JavabaseBM.unpinPage(leafPageID, true);

	}

	public int getKeySize() {
		return keySize;
	}

	public int getKeyType() {
		return keyType;
	}
}