/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc = pf.open(indexname, mode);
	if (rc < 0) {
		rootPid = -1;
		return rc;
	}

	rootPid = 0;

	if (pf.endPid() <= 0) {
		BTLeafNode leaf;
		return leaf.write(rootPid, pf);
	}

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	rootPid = -1;
    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	int currentHeight = 0;
	int siblingKey;
	int siblingPid;
	return insertHelper(rootPid, key, rid, currentHeight, siblingPid, siblingKey);
}

RC BTreeIndex::insertHelper(PageId pid, int key, const RecordId& rid, 
						    int curHeight, int& siblingPid, int& siblingKey)
{
	if (curHeight < treeHeight)
	{
		//not at a leaf node yet
		int rc = 0;
		BTNonLeafNode curHead;
		rc = curHead.read(pid, pf);
		if (rc != 0) {
			return rc;
		}

		curHead.locateChildPtr(key, pid);

		rc = insertHelper(pid, key, rid, curHeight+1, siblingPid, siblingKey); //rc will be NODE_FULL if it split
		
		if (rc == RC_NODE_FULL && curHeight != 0) //not the root
		{//check to split the "parent" node
			int rc2 = curHead.insert(siblingKey, siblingPid);
			if(rc2 == RC_NODE_FULL)
			{
				BTNonLeafNode newNode;
				siblingPid = pf.endPid();
				curHead.insertAndSplit(key, pid, newNode, siblingKey);

				curHead.write(pid, pf);
				newNode.write(siblingPid, pf);
				
				//create new child node, set the right hand pointer to it
			} else if (rc2 == 0) {
				return curHead.write(pid, pf);
			}

			return rc2; //RC_NODE_FULL
		}
		else if (rc == RC_NODE_FULL)
		{
			int rc2 = curHead.insert(siblingKey, siblingPid);
			if(rc2 == RC_NODE_FULL)
			{
				BTNonLeafNode newNode;
				siblingPid = pf.endPid();
				curHead.insertAndSplit(key, pid, newNode, siblingKey);
				BTNonLeafNode newRoot;
				rootPid = pf.endPid();
				
				newRoot.initializeRoot(pid, siblingKey, siblingPid);

				treeHeight++;
				
				newRoot.write(rootPid, pf);
				curHead.write(pid, pf);
				newNode.write(siblingPid, pf);
				return 0;

			} else if (rc2 == 0) {
				return curHead.write(pid, pf);
			}

			return rc2;		
		}
	} else {
		//at a leaf node

		int rc = 0;
		BTLeafNode curHead;
		rc = curHead.read(pid, pf);
		if(rc != 0)
		{
			return rc;
		}

		rc = curHead.insert(key, rid);
		if(rc == 0)
		{
			// Success, easiest case, insert works
			return curHead.write(pid, pf);
		}
		else if (rc == RC_NODE_FULL)
		{
			// Need to insert and split
			BTLeafNode newNode;
			siblingPid = pf.endPid();

			curHead.insertAndSplit(key, rid, newNode, siblingKey);
			
			// Need to set the sibling pointer
			curHead.setNextNodePtr(siblingPid);

			curHead.write(pid, pf);
			newNode.write(siblingPid, pf);
		}
		return rc;
	}

}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	RC rc;

	// If the tree is empty, return error
	if (treeHeight == 0) {
		return RC_NO_SUCH_RECORD;
	}

	// Initialize a temp node
	BTNonLeafNode curHead;
	PageId pid = rootPid;
	//curHead.initalizeRoot(0, -1, -1);
	for (int curHeight = 1; curHeight < treeHeight; curHeight++) {
		// Read contents of PageFile into NonLeafNode
		rc = curHead.read(pid, pf);
		if (rc != 0) {
			return rc;
		}
		// Find the leaf node by traversing through the tree
		rc = curHead.locateChildPtr(searchKey, pid);
		if (rc != 0) {
			return rc;
		}
	}
	
	// Initialize a new leaf node
	BTLeafNode leaf;
	// Read contents of PageFile into LeafNode
	rc = leaf.read(pid, pf);
	if (rc != 0) {
		return rc;
	}
	// Find eid of LeafNode that contains searchKey
	rc = leaf.locate(searchKey, cursor.eid);
	if (rc != 0) {
		return rc;
	}

	// Load the pid of the LeafNode into the cursor
	cursor.pid = pid;
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	//node is a temp node that we can read from
	BTLeafNode node;
	int nodeSize = (sizeof(RecordId) + sizeof(int));

	//return code
	int rc = 0;
	rc = node.read(cursor.pid, pf);

	//if rc has an error code
	if( rc != 0) {
		return rc;
	}

	//start of the buffer
	char* tempBuffer = node.getBuffer();
	tempBuffer += nodeSize * cursor.eid;
	//start of key, copy record
	memcpy(&rid, tempBuffer, sizeof(RecordId));
	tempBuffer += sizeof(RecordId);
	//copy key
	memcpy(&key, tempBuffer, sizeof(int));

	//set it to the end, where the next sibling pid will be
	tempBuffer = node.getBuffer() + (node.getKeyCount() * nodeSize);

	if(cursor.eid == node.getKeyCount())
	{
		//set pid to the next sibling, set eid to 0
		memcpy(&(cursor.pid), tempBuffer, sizeof(PageId));
		cursor.eid = 0;
		return 0;
	}
	else
	{
		//just increment eid
		cursor.eid++;
		return 0;
	}
}
