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
	// Open the index
	RC rc = pf.open(indexname, mode);
	if (rc != 0) {
		return rc;
	}

	// If no pid, initialize a new root node
	if (pf.endPid() != 0) {
		char buffer[PageFile::PAGE_SIZE];
		// Read pid 0 into a buffer to get rootPid and treeHeight
		if (pf.read(0, buffer) != 0) {
			fprintf(stdout, "Could not read index %s", indexname);
			return RC_FILE_READ_FAILED;
		}

		char* iter = &(buffer[0]);
		memcpy(&rootPid, iter, sizeof(PageId));
		iter += sizeof(PageId);
		memcpy(&treeHeight, iter, sizeof(int));
	}

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	// Write rootPid and treeheight to a buffer
	char buffer[PageFile::PAGE_SIZE];
	char* iter = &(buffer[0]);
	memset(iter, 0, PageFile::PAGE_SIZE);
	memcpy(iter, &rootPid, sizeof(PageId));
	iter += sizeof(PageId);
	memcpy(iter, &treeHeight, sizeof(PageId));

	// Write the buffer to pid 0
	if (pf.write(0, buffer) != 0) {
		return RC_FILE_WRITE_FAILED;
	}
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
	RC rc;
	// Check if tree has been initialized, if not, initialize
	if (treeHeight == 0) {
		BTLeafNode rootLeaf;
		rootLeaf.insert(key, rid);
		rootPid = pf.endPid();
		rc = rootLeaf.write(rootPid, pf);
		treeHeight++;
	}
	// Else, insert new key finding correct location
	else {
		int startHeight = 1;
		int siblingKey;
		int siblingPid;
		rc = insertHelper(rootPid, key, rid, currentHeight, siblingPid, siblingKey);
	}
    return rc;
}

RC BTreeIndex::insertHelper(PageId pid, int key, const RecordId& rid,
	int curHeight, int& siblingPid, int& siblingKey) {

	RC rc;

	// If we are not at the leaf nodes
	if (curHeight < treeHeight) {
		BTNonLeafNode curHead;
		rc = curHead.read(pid, pf);
		if (rc != 0) {
			return rc;
		}

		// Locate the correct child ptr to traverse
		PageId child;
		curHead.locateChildPtr(key, child);

		// Recursively call insertHelper with the child node
		rc = insertHelper(child, key, rid, curHeight + 1, siblingPid, siblingKey);

		// If we return a node full from our child, insert key from child
		if (rc == RC_NODE_FULL && curHeight > 1) {
			rc = curHead.insert(siblingKey, siblingPid);
			// If we return full from insert, we must insertAndSplit
			if (rc == RC_NODE_FULL) {
				BTNonLeafNode sibling;
				int n_siblingKey;
				// insertAndSplit on the curHead
				curHead.insertAndSplit(siblingKey, siblingPid, sibling, n_siblingKey);
				PageId n_siblingPid = pf.endPid();

				// Write the curHead and sibling to PageFile
				curHead.write(pid, pf);
				sibling.write(n_siblingPid, pf);

				// Set the pid and key to pass back up
				siblingPid = n_siblingPid;
				siblingKey = n_siblingKey;
			} else if (rc == 0) {
				return curHead.write(pid, pf);
			}

			return rc;
		}
		// Child passed up key and pid to nonleaf root
		else if (rc == RC_NODE_FULL) {
			rc = curHead.insert(siblingKey, siblingPid);
			// If we return full from insert, insertAndSplit root
			if (rc == RC_NODE_FULL) {
				BTNonLeafNode sibling;
				int n_siblingKey;
				// insertAndSplit on the current root
				curHead.insertAndSplit(siblingKey, siblingPid, sibling, n_siblingKey);
				PageId n_siblingPid = pf.endPid();

				BTNonLeafNode newRoot;
				rootPid = pf.endPid();
				newRoot.initializeRoot(pid, n_siblingKey, n_siblingPid);
				treeHeight++;

				newRoot.write(rootPid, pf);
				curHead.write(pid, pf);
				sibling.write(n_siblingPid, pf);

				return 0;
			} else if (rc == 0) {
				return curHead.write(pid, pf);
			}

			return rc;
		}
	}
	// Or we are at a LeafNode
	else {
		BTLeafNode curHead;
		rc = curHead.read(pid, pf);
		if (rc != 0) {
			return rc;
		}

		rc = curHead.insert(key, rid);
		// If we return full from insert, insertAndSplit LeafNode
		if (rc == RC_NODE_FULL) {
			BTLeaNode sibling;
			curHead.insertAndSplit(key, rid, sibling, siblingKey);
			siblingPid = pf.endPid();

			// If this LeafNode was our first root, initialize root
			if (treeHeight == 1) {
				BTNonLeafNode firstRoot;
				firstRoot.initializeRoot(pid, siblingKey, siblingPid);
				treeHeight++;
				firstRoot.write(rootPid, pf);
			}

			// Need to set the sibling pointer
			curHead.setNextNodePtr(siblingPid);
			curHead.write(pid, pf);
			sibling.write(siblingPid, pf);
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
	// If tree is empty, return error
	if (treeHeight == 0) {
		return RC_END_OF_TREE;
	}

	RC rc;

	// Initialize a temp node
	BTNonLeafNode curHead;
	PageId pid = rootPid;
	for (int curHeight = 1; curHeight < treeHeight; curHeight++) {
		// Read PageFile into NonLeafNode
		rc = curHead.read(pid, pf);
		if (rc != 0) {
			return rc;
		}
		// Find leaf node by locating the correct child ptr
		rc = curHead.locateChildPtr(searchKey, pid);
		if (rc != 0) {
			return rc;
		}
	}

	// Once we get the pid of a leaf node, initialize a new leaf
	BTLeafNode leaf;
	// Read PageFile into LeafNode
	rc = leaf.read(pid, pf);
	if (rc != 0) {
		return rc;
	}
	// Find the eid of LeafNode that contains searchKey or greater
	rc = leaf.locate(searchKey, cursor.eid);
	if (rc != 0) {
		return rc;
	}

	// Load pid of the LeafNode into the cursor
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
	// The size of a node in a LeafNode
	int nodeSize = sizeof(RecordId) + sizeof(int);
	BTLeafNode node;

	// Read PageFile into LeafNode
	RC rc = node.read(cursor.pid, pf);
	if (rc != 0) {
		return rc;
	}

	// Read the entry our current cursor is at
	rc = node.readEntry(cursor.eid, key, rid);
	// Move the cursor forward
	cursor.eid++;

	int tempKey;
	RecordId tempRid;
	// If the next entry gives us RC_NO_SUCH_RECORD, move cursor to next node
	if (node.readEntry(cursor.eid, tempKey, tempRid) == RC_NO_SUCH_RECORD) {
		cursor.eid = 0;
		cursor.pid = node.getNextNodePtr();
	}

    return 0;
}
