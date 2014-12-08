#include "BTreeNode.h"
#include <climits>
#include <iostream>

using namespace std;
// BTLeafNode constructor.  Keeps track of the current key count; be sure to
//	increment whenever adding a new element

const int BTLeafNodeMaxSize = (PageFile::PAGE_SIZE - sizeof(int)*2)/(sizeof(int) + sizeof(RecordId));
const int NULL_VALUE = INT_MIN; //int min +1 for portability
const int BTLeafNodeSize = sizeof(int) + sizeof(RecordId);

const int BTNonLeafNodeMaxSize = (PageFile::PAGE_SIZE - sizeof(int)*2)/(sizeof(int) + sizeof(PageId));
const int BTNonLeafNodeSize = sizeof(PageId) + sizeof(int);


BTLeafNode::BTLeafNode()
{
	setKeyCount(0);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	char* iter = &(buffer[0]);
	int keyCount = 0;
	memcpy(&keyCount, iter, sizeof(int));
	return keyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	char* iter = getBuffer();
	iter += sizeof(int);
	int keyCount = getKeyCount();

	if(keyCount >= BTLeafNodeMaxSize)
	{
		return RC_NODE_FULL;
	}

	int pos;
	RC rc = locate(key, pos); //if it's in the first slot, it'll return 0

	if (rc == 0)
	{
		iter += (pos * BTLeafNodeSize); //get to the appropriate place to move
	}
	else if(rc == RC_NO_SUCH_RECORD)
	{
		iter += (getKeyCount()* BTLeafNodeSize);
		rc = 0;
	}

	memmove(iter + BTLeafNodeSize, iter, ((keyCount - pos)*BTLeafNodeSize) + sizeof(PageId));
	//moves entire data set over by one node
	memcpy(iter, &rid, sizeof(RecordId));
	memcpy(iter + sizeof(RecordId), &key, sizeof(int));

	incrementKeyCount();
//Rid, key
	return rc;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid,
                              BTLeafNode& sibling, int& siblingKey)
{
	RC rc;
	int keyCount = getKeyCount();
	int splitAmount = keyCount/2; //should get 42
	char* iter = &(buffer[0]);

	iter += sizeof(int); //go past keycounter

	iter += splitAmount*BTLeafNodeSize; //go to first recordId node

	char* sibIter = sibling.getBuffer();
	int copyAmount = keyCount - splitAmount;

	//memcpy(sibIter, &copyAmount, sizeof(int)); //sets sister keycount
	sibling.setKeyCount(copyAmount);
	sibIter += sizeof(int);

	//printBuffer();

	memcpy(sibIter, iter, (BTLeafNodeSize*copyAmount) + sizeof(PageId)); //copy data over
	memcpy(&siblingKey, iter + sizeof(RecordId), sizeof(int)); //sets siblingKey

	//TODO set key count
	setKeyCount(splitAmount);

	if(key > siblingKey)
	{
		rc = sibling.insert(key, rid);
	}
	else
	{
		rc = insert(key, rid);
	}
	return rc;
}

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	int i = 0;

	char* iter = getBuffer();
	int keyCount = getKeyCount();
	int checkKey = 0;
	iter += sizeof(int);
	iter += sizeof(RecordId);

	if (keyCount == 0)
	{
		eid = 0;
		return RC_NO_SUCH_RECORD;
	}

	for(i = 0; i < keyCount; i++)
	{
		memcpy(&checkKey, iter, sizeof(int));
		if(checkKey >= searchKey)
		{
			memcpy(&eid, &i, sizeof(int));
			return 0;
		}
		iter += BTLeafNodeSize;
	}

	eid = keyCount;
	return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	char* iter = getBuffer();
	iter += sizeof(int);

	int i = 0;

	if(eid > getKeyCount())
	{
		return RC_NO_SUCH_RECORD;
	}

	iter += (BTLeafNodeSize * eid);
	memcpy(&rid, iter, sizeof(RecordId));
	iter += sizeof(RecordId);
	memcpy(&key, iter, sizeof(int));

	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node
 */
PageId BTLeafNode::getNextNodePtr()
{
	char* iter = getBuffer();
	iter += sizeof(int);
	iter += (BTLeafNodeSize * getKeyCount());
	int nextPid;
	memcpy(&nextPid, iter, sizeof(int));

	return nextPid;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	char* iter = getBuffer();
	iter += sizeof(int);
	iter += (BTLeafNodeSize * getKeyCount());

	memcpy(iter, &pid, sizeof(int));
}


////********************************************************////

BTNonLeafNode::BTNonLeafNode()
{
	setKeyCount(0);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
 return pf.read(pid, buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
 	char* iter = getBuffer();
	int keyCount = 0;
	memcpy(&keyCount, iter, sizeof(int));
	return keyCount;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	if(getKeyCount() >= BTNonLeafNodeMaxSize)
	{
		return RC_NODE_FULL;
	}
	char* iter = getBuffer();

	int counter = 0;
	iter += sizeof(int);
	iter += sizeof(PageId);
	int checker = 0;

	for(counter = 0; counter < getKeyCount(); counter++)
	{
		memcpy(&checker, iter, sizeof(int));
		if(checker > key)
		{

			break;
		}
		iter += BTNonLeafNodeSize;
	}

	memmove(iter + BTNonLeafNodeSize, iter, ((getKeyCount() - counter) * BTLeafNodeSize));

	memcpy(iter, &key, sizeof(int));
	memcpy(iter + sizeof(int), &pid, sizeof(PageId));

	incrementKeyCount();

	return 0;

}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
	RC rc;

	int keyCount = getKeyCount();
	int splitAmount = keyCount / 2;

	char* iter = getBuffer();
	iter += sizeof(int);
	iter += sizeof(PageId); //first PageId


	iter += splitAmount*BTNonLeafNodeSize; //go to first recordId node
	memcpy(&midKey, iter, sizeof(int));
	iter += sizeof(int);

	char* sibIter = sibling.getBuffer();
	sibIter += sizeof(int); //keyCount

	int copyAmount = keyCount - splitAmount;

	memcpy(sibIter, iter, (BTNonLeafNodeSize * copyAmount) + sizeof(PageId)); //copy data over
	sibling.setKeyCount(copyAmount); //sets sister keycount

	setKeyCount(splitAmount);

	if(key > midKey)
	{
		rc = sibling.insert(key, pid);
	}
	else
	{
		rc = insert(key, pid);
	}
	return rc;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	char* iter = getBuffer();
	iter += sizeof(int);
	iter += sizeof(PageId);

	int checker;
	memcpy(&checker, iter, sizeof(int));

	if(checker > searchKey)
	{
		memcpy(&pid, iter - sizeof(PageId), sizeof(PageId));
		return 0;
	}

	for(int i = 0; i < getKeyCount(); i++)
	{
		if(checker > searchKey)
		{
			memcpy(&pid, iter - sizeof(PageId), sizeof(PageId));
			return 0;
		}
		else if (checker == searchKey)
		{
			memcpy(&pid, iter + sizeof(int), sizeof(PageId));
			return 0;
		}
		iter += BTNonLeafNodeSize;
		memcpy(&checker, iter, sizeof(int));
	}

	iter -= sizeof(PageId);

	memcpy(&pid, iter, sizeof(PageId));
	return 0;

}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	incrementKeyCount();
	char* iter = getBuffer();
	iter += sizeof(int);
	memcpy(iter, &pid1, sizeof(PageId));
	iter += sizeof(PageId);
	memcpy(iter, &key, sizeof(int));
	iter += sizeof(int);
	memcpy(iter, &pid2, sizeof(PageId));
	return 0;
}

void BTNonLeafNode::printBuffer()
{

	cout << "Print Buffer ===================================\n";
	int* p = (int*) buffer;
	int key = 0;
	int pid = *(p);
	for(int i = 0; i < PageFile::PAGE_SIZE/sizeof(int); i++)
	{
		cout << "buffer [" << i << "] : " << *(p + i) << endl;
	}


	/*cout << "[" << pid << "]";
	p = p + 1;
	for (int i = 0; i < keyCount; i++)
	{
		int key = 0;
		RecordId rid;
		//readLeafEntry(i, key, rid);
		int *p = (int*) buffer;
		p++;
		p = p + i*2;
		key = *p;
		int pid = *(p + 1);


		cout << "[" << key << "]";
		cout << "[" << pid << "]";
		// cout << "[" << rid.pid << "]";
		// cout << "[" << rid.sid << "]\n";
	}*/
	cout << "End Buffer =====================================\n";
}

void BTLeafNode::printBuffer()
{

	cout << "Print Buffer ===================================\n";
	int *p = (int*) buffer;
	int key = 0;
	for (int i = 0; i < PageFile::PAGE_SIZE/sizeof(int); i++)
	{
		RecordId rid;
		//readLeafEntry(i, key, rid);
		key = *p;
		cout << "buffer[" << i << "] is equal to " << key << endl;
		p++;
		// cout << "[" << rid.pid << "]";
		// cout << "[" << rid.sid << "]\n";
	}


	cout << "Stack checker: " << "buffer[256] is equal to " << key << endl;
	cout << "End Buffer =====================================\n";
}
