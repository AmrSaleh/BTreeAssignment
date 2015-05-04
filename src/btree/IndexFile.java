package btree;

import java.io.*;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.InvalidSlotNumberException;
import heap.SpaceNotAvailableException;

/**
 * Base class for a index file
 */
public abstract class IndexFile 
{
  /**
   * Insert entry into the index file.
   * @param data the key for the entry
   * @param rid the rid of the tuple with the key
 * @throws InsertRecException 
 * @throws IOException 
 * @throws InvalidFrameNumberException 
 * @throws HashEntryNotFoundException 
 * @throws PageUnpinnedException 
 * @throws ReplacerException 
 * @throws ConstructPageException 
 * @throws InvalidSlotNumberException 
 * @throws ConvertException 
 * @throws NodeNotMatchException 
 * @throws KeyNotMatchException 
 * @throws BufMgrException 
 * @throws PagePinnedException 
 * @throws BufferPoolExceededException 
 * @throws PageNotReadException 
 * @throws HashOperationException 
 * @throws DeleteRecException 
 * @throws SpaceNotAvailableException 
   */
  abstract public void insert(final KeyClass data, final RID rid) throws InsertRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, IOException, ConstructPageException, InvalidSlotNumberException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, KeyNotMatchException, NodeNotMatchException, ConvertException, DeleteRecException, SpaceNotAvailableException;
  
  /**
   * Delete entry from the index file.
   * @param data the key for the entry
   * @param rid the rid of the tuple with the key
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
 * @throws DeleteRecException 
   */
  abstract public boolean Delete(final KeyClass data, final RID rid) throws InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, HashEntryNotFoundException, IOException, DeleteRecException;
}
