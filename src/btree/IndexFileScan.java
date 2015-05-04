package btree;

import heap.InvalidSlotNumberException;

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


/**
 * Base class for a index file scan
 */
public abstract class IndexFileScan 
{
  /**
   * Get the next record.
   * @return the KeyDataEntry, which contains the key and data
 * @throws IOException 
 * @throws HashEntryNotFoundException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws ConstructPageException 
 * @throws BufMgrException 
 * @throws PagePinnedException 
 * @throws BufferPoolExceededException 
 * @throws PageNotReadException 
 * @throws InvalidFrameNumberException 
 * @throws PageUnpinnedException 
 * @throws HashOperationException 
 * @throws ReplacerException 
 * @throws InvalidSlotNumberException 
   */
  abstract public KeyDataEntry get_next() throws InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, HashEntryNotFoundException, IOException;

  /** 
   * Delete the current record.
 * @throws HashEntryNotFoundException 
 * @throws ConstructPageException 
 * @throws BufMgrException 
 * @throws PagePinnedException 
 * @throws BufferPoolExceededException 
 * @throws PageNotReadException 
 * @throws InvalidFrameNumberException 
 * @throws PageUnpinnedException 
 * @throws HashOperationException 
 * @throws ReplacerException 
 * @throws IOException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws InvalidSlotNumberException 
 * @throws DeleteRecException 
   */
   abstract public void delete_current() throws InvalidSlotNumberException, KeyNotMatchException, NodeNotMatchException, ConvertException, IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, HashEntryNotFoundException, DeleteRecException;

  /**
   * Returns the size of the key
   * @return the keysize
   */
  abstract public int keysize();
}
