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
#include <climits>

using namespace std;

const int NULL_VALUE = INT_MIN; //int min +1 for portability

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
	if (rc != 0) {
		rootPid = -1;
		return rc;
	}

	//rootPid = 0;

	// If no pid initialize a new root node
	if (pf.endPid() != 0) {
		char buffer[PageFile::PAGE_SIZE];
		if (pf.read(0, buffer) != 0) {
			fprintf(stdout, "Could not read\n");
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
	//rootPid = -1;
	char buffer[PageFile::PAGE_SIZE];
	char* iter = &(buffer[0]);
	memset(iter, 0, PageFile::PAGE_SIZE);
	memcpy(iter, &rootPid, sizeof(PageId));
	iter += sizeof(PageId);
	memcpy(iter, &treeHeight, sizeof(PageId));
	if( pf.write(0, buffer) != 0) {
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
RC BTreeIndex::insert(int key, const RecordId& rid){
	RC rc = 0;
    if (treeHeight == 0)
    {
    	BTLeafNode newRoot;
    	newRoot.insert(key, rid);
    	rootPid = pf.endPid();

    	rc = newRoot.write(rootPid, pf);
    	treeHeight = 1;
    }
	else
	{
    	int currentHeight = 1;
    	int siblingKey;
    	PageId siblingPid;
    	rc = insertHelper(rootPid, key, rid, currentHeight, siblingPid, siblingKey);
    }
    return rc;
}


RC BTreeIndex::insertHelper(PageId pid, int key, const RecordId& rid,
                            int curHeight, int& siblingPid, int& siblingKey)
{
    if (curHeight < treeHeight)
    {
        BTNonLeafNode curHead;
        curHead.read(pid, pf);

        PageId nextPid, splitPid;
        int splitKey;
        curHead.locateChildPtr(key, nextPid);

        RC rc = insertHelper(nextPid, key,rid, curHeight+1, splitPid, splitKey);

        if (rc == RC_NODE_FULL)
        {
            rc = curHead.insert(splitKey, splitPid);
            if (rc == 0)
            {
                curHead.write(pid, pf);
                return rc;
            }
            else
            {
                rc = RC_NODE_FULL;
                BTNonLeafNode newNode;
                int midKey;
                curHead.insertAndSplit(splitKey, splitPid, newNode, midKey);

                PageId sibId = pf.endPid();
                newNode.write(sibId, pf);
                curHead.write(pid, pf);

                if ( curHeight == 1)
                {
                	BTNonLeafNode firstRoot;
                	rootPid = pf.endPid();
                	rc = firstRoot.initializeRoot(pid, siblingKey, siblingPid);
                	treeHeight++;
                	firstRoot.write(rootPid, pf);
            	}

                siblingPid = sibId;
                siblingKey = midKey;
                return rc;
            }
        }
        else
        	return rc;

    }
    else
    {
        BTLeafNode curHead;
        curHead.read(pid, pf);

        RC rc = curHead.insert(key, rid);

        if (rc == 0)
        {
            curHead.write(pid, pf);
            return rc;
        }
        else if (rc == RC_NODE_FULL)
        {
            BTLeafNode newNode;
            //sn.cleanStart();
            int tempSibKey;
            newNode.insertAndSplit(key, rid, newNode, tempSibKey);

            PageId tempSibPid = pf.endPid();
            newNode.write(tempSibPid, pf);

            curHead.setNextNodePtr(tempSibPid);
            curHead.write(pid, pf);

            siblingPid = tempSibPid;
            siblingKey = tempSibKey;
			if ( curHeight == 1)
            {
            	BTNonLeafNode firstRoot;
            	rootPid = pf.endPid();
            	rc = firstRoot.initializeRoot(pid, siblingKey, siblingPid);
            	treeHeight++;
            	firstRoot.write(rootPid, pf);

        	}
            return rc;
        }
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

	if (cursor.eid == 0) {
		fprintf(stdout, "CURRENT PAGE ID IS %d\n", cursor.pid);
		fprintf(stdout, "Height is %d\n", treeHeight);
		node.printBuffer();
	}

	//if rc has an error code
	if( rc != 0) {
		return rc;
	}

	//start of the buffer
	//char* tempBuffer = node.getBuffer();
	//tempBuffer += nodeSize * cursor.eid;
	//start of key, copy record
	//memcpy(&rid, tempBuffer, sizeof(RecordId));
	//tempBuffer += sizeof(RecordId);
	//copy key
	//memcpy(&key, tempBuffer, sizeof(int));

	rc = node.readEntry(cursor.eid, key, rid);
	cursor.eid++;

	int testKey;
	RecordId testRid;
	if(node.readEntry(cursor.eid, testKey, testRid) == RC_NO_SUCH_RECORD) {
		cursor.eid = 0;
		cursor.pid = node.getNextNodePtr();
	}

	return 0;

	//set it to the end, where the next sibling pid will be
	//tempBuffer = node.getBuffer() + (node.getKeyCount() * nodeSize);

	/*fprintf(stdout, "The keyCount is %d\n", node.getKeyCount());
	fprintf(stdout, "The eid is %d\n", cursor.eid);
	if(cursor.eid > node.getKeyCount() - 1)
	{
		//set pid to the next sibling, set eid to 0
		//memcpy(&(cursor.pid), tempBuffer, sizeof(PageId));
		cursor.pid = node.getNextNodePtr();
		fprintf(stdout, "The next node ptr is %d\n", cursor.pid);
		cursor.eid = 0;
		return 0;
	}
	else
	{
		//just increment eid
		cursor.eid++;
		return 0;
	}*/
}

void BTreeIndex::printTree()
{
	printRecurse(rootPid, 1);
}

void BTreeIndex::printRecurse(PageId pid, int level)
{
	cout << "\n========================================\n";
	cout << "Printing out level: " << level << endl;
	cout << "Printing out page pid: " << pid << endl;
	if (level > treeHeight)
		return;
	// Leaf node
	else if (level == treeHeight)
	{
		cout << "Printing out leaf node!" << endl;
		BTLeafNode node;
		node.read(pid, pf);
		RecordId rid;
		int key = -1;
		for (int i = 0; i < node.getKeyCount(); i++)
		{
			node.readEntry(i, key, rid);
			cout << "[" << key << "]";
		}
	}
	else
	{
		cout << "Printing out nonleaf node!" << endl;
		BTNonLeafNode node;

		node.read(pid, pf);
		//int key = -1;
		/*for (int i = 0; i < node.getKeyCount(); i++)
		{
			node.readNonLeafEntry(i, key);
			cout << "[" << key << "]";
		}*/
		node.printBuffer();

		int key = -1;
		//node.readNonLeafEntry(0, key);
		char* iter = node.getBuffer() + sizeof(PageId);
		memcpy(&key, iter, sizeof(int));
		PageId child;
		node.locateChildPtr(key-1, child);
		printRecurse(child, level+1);

		char* iter2 = node.getBuffer() + sizeof(PageId);

		for (int i = 0; i < node.getKeyCount(); i++)
		{
			key = -1;
			//node.readNonLeafEntry(i, key);
			memcpy(&key, iter2, sizeof(int));
			iter2 += sizeof(int) + sizeof(PageId);
			PageId child_page;
			node.locateChildPtr(key, child_page);
			printRecurse(child_page, level+1);
		}

	}
	cout << "\n========================================\n";
}
