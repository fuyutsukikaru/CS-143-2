#include "BTreeNode.h"

using namespace std;

// BTLeafNode constructor.  Keeps track of the current key count; be sure to
//	increment whenever adding a new element
BTLeafNode::BTLeafNode()
{
	keyCount = 0;
	nextPid = 0;
	int i = 0;
	char* iter = &(buffer[0]);
	const int neg = -1;
	while(i < sizeof(buffer)) {
		memcpy(iter, &neg, sizeof(int));
		i += sizeof(int);
		iter += sizeof(int);
	}
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	RC ret =  pf.read(pid, buffer);
	char* iter = &(buffer[0]);
	int check;
	int nodeSize = sizeof(RecordId) + sizeof(int);
	memcpy(&check, iter, sizeof(int));
	while (check != -1) {
		keyCount++;
		iter += nodeSize;
		memcpy(&check, iter, sizeof(int));

	}

	return ret;
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
	fprintf(stdout, "Key count is %d\n", keyCount);
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
	int nodeSize = sizeof(int) + sizeof(RecordId);
	int maxSize = ((PageFile::PAGE_SIZE) - sizeof(PageId))/nodeSize;
	if (keyCount >= maxSize) {
		fprintf(stderr, "Error: nodes are full\n");
		return RC_NODE_FULL;

	} else {
		int position;
		char* iter = &(buffer[0]);

		if (locate(key, position) == RC_NO_SUCH_RECORD) {
			position = keyCount;  //-1 because keycount was incremented prior to the check
			//position should contain where the stuff should go
		}
		keyCount++;

		memmove(buffer + position*nodeSize + nodeSize, buffer + position*nodeSize, nodeSize*((keyCount-1)-position) + sizeof(PageId));
		memcpy(buffer + position*nodeSize, &rid, sizeof(RecordId));
		memcpy(buffer + position*nodeSize + sizeof(RecordId), &key, sizeof(int));

		fprintf(stdout, "Successfully wrote node with key: %d, RecordId pid: %d, sid: %d\n",
			key, (int)rid.pid, rid.sid);

		return 0;
	}
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
	int nodeSize = sizeof(int) + sizeof(RecordId);
	int splitter = keyCount / 2;
	char* iter = &(buffer[0]);
	int position = 0;
	if (locate(key, position) == RC_NO_SUCH_RECORD) {
		position = keyCount;
	}

	iter += splitter*nodeSize;
	memcpy(sibling.getBuffer(), iter, (keyCount - splitter)*nodeSize + sizeof(PageId));

	sibling.setKeyCount(keyCount - splitter);
	keyCount = splitter;

	if (iter > &(buffer[PageFile::PAGE_SIZE/2 - 1]))  //add to the right
		sibling.insert(key, rid);
	else
		insert(key, rid);

	memcpy(&siblingKey, sibling.getBuffer() + sizeof(RecordId), sizeof(key));
	sibling.setNextNodePtr(getNextNodePtr());
	setNextNodePtr(getNextNodePtr());
	return 0;
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
	int nodeSize =  sizeof(RecordId) + sizeof(int);
	int key = 0;
	int cur;
	char* iter = &(buffer[0]);
	iter += sizeof(RecordId);

	while(key < keyCount) {
		memcpy(&cur, iter, sizeof(int));
		if (cur >= searchKey) {
			eid = key;
			return 0;
		}
		key++;
		iter += nodeSize;
	}

	eid = -1;
	fprintf(stderr, "Error: The record does not exist\n");
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
	int nodeSize =  sizeof(RecordId) + sizeof(int);
	char* iter = &(buffer[0]);
	int i = 0;

	if (eid > keyCount) {
		fprintf(stderr, "Error: The record does not exist\n");
		return RC_NO_SUCH_RECORD;
	}

	while (i < eid) {
		iter += nodeSize;
		i++;
	}

	memcpy(&rid, iter, sizeof(RecordId));
	iter += sizeof(RecordId);
	memcpy(&key, iter, sizeof(int));

	return 0;
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node
 */
PageId BTLeafNode::getNextNodePtr()
{
	return nextPid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	nextPid = pid;
	int nodeSize = sizeof(RecordId) + sizeof(int);
	memcpy(buffer + nodeSize * keyCount, &pid, sizeof(PageId));
	return 0;
}
 //*******************************************************************//

// BTLeafNode constructor.  Keeps track of the current key count; be sure to
//	increment whenever adding a new element
BTNonLeafNode::BTNonLeafNode()
{
	keyCount = 0;
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
	int nodeSize = sizeof(PageId) + sizeof(int);
	char* iter = &(buffer[0]);
	int cur = 0;

	if((keyCount+1) * nodeSize + sizeof(int) >= sizeof(buffer))
		return RC_NODE_FULL;

	iter += sizeof(PageId);
	memcpy(&cur, iter, sizeof(int));

	while(cur < key) //iterate the iter until you reach a key that's greater or equal
	{
		iter += nodeSize;
		memcpy(&cur, iter, sizeof(int));
	}

	if(&(buffer[PageFile::PAGE_SIZE - 1]) - iter < sizeof(PageId) + sizeof(int))
		return RC_NODE_FULL;

	memmove(iter + nodeSize, iter, &(buffer[PageFile::PAGE_SIZE - 1]) - iter - nodeSize);
	memcpy(iter, &key, sizeof(int));
	memcpy(iter + sizeof(int), &pid, sizeof(PageId));

	keyCount++;
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
	int nodeSize = sizeof(PageId) + sizeof(int);
	char* iter = &(buffer[0]);
	int cur = 0;
	int splitter = keyCount/ 2;
	char* splitPoint = iter + splitter * nodeSize;

	if (splitter == 0)
		return RC_NODE_FULL;//actually empty

	iter += sizeof(PageId);
	memcpy(&cur, iter, sizeof(int));

	while(cur < key) //iterate the iter until you reach a key that's greater or equal
	{
		iter += nodeSize;
		memcpy(&cur, iter, sizeof(int));
	}

	memcpy(sibling.getBuffer(), iter - sizeof(PageId), (keyCount - splitter)*nodeSize + sizeof(PageId));
	sibling.setKeyCount(keyCount - splitter);
	keyCount = splitter;

	memcpy(&midKey, splitPoint, sizeof(int));

	if (iter > &(buffer[PageFile::PAGE_SIZE/2 - 1]))  //add to the right
		sibling.insert(key, pid);
	else
		insert(key, pid);

	return 0;
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
	int nodeSize =  sizeof(PageId) + sizeof(int);
	char* iter = &(buffer[0]);
	int key = 0;
	int cur = 0;

	iter += sizeof(PageId);
	memcpy(&cur, iter, sizeof(int));
	if (keyCount > 0 && searchKey < cur) {
		iter -= sizeof(PageId);
		memcpy(&pid, iter, sizeof(PageId));
		return 0;
	}

	while (key < keyCount) {
		memcpy(&cur, iter, sizeof(int));

		if (cur >= searchKey) {
			iter += sizeof(int);
			memcpy(&pid, iter, sizeof(PageId));
			return 0;
		}
		key++;
		iter += nodeSize;
	}

	iter += sizeof(key);
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
	char* iter = &(buffer[0]);
	memcpy(iter, &pid1, sizeof(PageId));
	iter += sizeof(PageId);
	memcpy(iter, &key, sizeof(int));
	iter += sizeof(int);
	memcpy(iter, &pid2, sizeof(PageId));
	keyCount++;
	return 0;
}
