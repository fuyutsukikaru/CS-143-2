#include "BTreeNode.h"
#include <iostream>

using namespace std;

// BTLeafNode constructor.  Keeps track of the current key count; be sure to
//	increment whenever adding a new element

const int NULL_VALUE = -2147483647; //int min +1 for portability
BTLeafNode::BTLeafNode()
{
	keyCount = 0;
	nextPid = 0;
	int i = 0;
	char* iter = &(buffer[0]);
	const int neg = NULL_VALUE;
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
	int i = 0;
	int nodeSize = sizeof(RecordId) + sizeof(int);
	memcpy(&check, iter, sizeof(int));
	while (check != NULL_VALUE && iter < &(buffer[PageFile::PAGE_SIZE])) {
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
	//fprintf(stdout, "Key count is %d\n", keyCount);
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
		//fprintf(stderr, "Error: nodes are full\n");
		return RC_NODE_FULL;

	} else {
		int position;
		char* iter = &(buffer[0]);

		if (locate(key, position) == RC_NO_SUCH_RECORD) {
			position = keyCount; //at the end if it can't be found
			//position should contain where the stuff should go
		}
		keyCount++;

		memmove(buffer + position*nodeSize + nodeSize, buffer + position*nodeSize, nodeSize*((keyCount-1)-position) + sizeof(PageId));
		memcpy(buffer + position*nodeSize, &rid, sizeof(RecordId));
		memcpy(buffer + position*nodeSize + sizeof(RecordId), &key, sizeof(int));

		//fprintf(stdout, "Successfully wrote node with key: %d, RecordId pid: %d, sid: %d\n",
		//	key, (int)rid.pid, rid.sid);

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

	while (iter < &(buffer[PageFile::PAGE_SIZE])) {
		iter += nodeSize;
		memcpy(iter, &NULL_VALUE, sizeof(int));
	}


	sibling.setKeyCount(keyCount - splitter);
	keyCount = splitter;

	if (&(buffer[0]) +(nodeSize*splitter) > &(buffer[PageFile::PAGE_SIZE/2 - 1]))  //add to the right
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
	//fprintf(stderr, "Error: The record does not exist\n");
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
		//fprintf(stderr, "Error: The record does not exist\n");
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
	int i = 0;
	char* iter = &(buffer[0]);
	const int neg = NULL_VALUE;
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

RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	RC ret =  pf.read(pid, buffer);
	char* iter = &(buffer[0]) + sizeof(PageId);
	int check;
	int nodeSize = sizeof(PageId) + sizeof(int);
	memcpy(&check, iter, sizeof(int));
	while (check != NULL_VALUE) {
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
	/*int nodeSize = sizeof(PageId) + sizeof(int);
	char* iter = &(buffer[0]);
	int cur = 0;
	int i = 0;

	fprintf(stdout, "Our size is %d\n", (keyCount+1) * nodeSize + sizeof(int));
	fprintf(stdout, "Max size is %d\n", PageFile::PAGE_SIZE);
	if ((keyCount+1) * nodeSize + sizeof(int) > PageFile::PAGE_SIZE) {
		return RC_NODE_FULL;
	}

	iter += sizeof(PageId);
	memcpy(&cur, iter, sizeof(int));

	while(cur < key  && i != keyCount) //iterate the iter until you reach a key that's greater or equal
	{
		i++;
		iter += nodeSize;
		memcpy(&cur, iter, sizeof(int));
	}

	if(&(buffer[PageFile::PAGE_SIZE - 1]) - iter < sizeof(PageId) + sizeof(int))
		return RC_NODE_FULL;

	if(i > 0)
	{
		memmove(iter + nodeSize, iter, &(buffer[PageFile::PAGE_SIZE - 1]) - iter - nodeSize);
		memcpy(iter, &key, sizeof(int));
		memcpy(iter + sizeof(int), &pid, sizeof(PageId));
	}
	else
	{
		iter -= sizeof(PageId); //move it to the beginning of the buffer
		memmove(iter + nodeSize, iter, &(buffer[PageFile::PAGE_SIZE - 1]) - iter - nodeSize);
		memcpy(iter, &pid, sizeof(PageId));
		memcpy(iter + sizeof(int), &key, sizeof(key));
	}
	keyCount++;
	return 0;*/
	PageId savedLastPid;
	const int maxKeysNL = (PageFile::PAGE_SIZE / 8) - 1;

    int keyCount = getKeyCount();

    if (keyCount == 126) {
        int* p = (int*) buffer + (2*keyCount);
        savedLastPid = *p;
    }

    if (keyCount >= maxKeysNL)
    	return RC_NODE_FULL;

    int maxKey = -1;

    if (keyCount > 0)
    {
    	int* e = (int*) buffer + (2*keyCount) - 1;
    	maxKey = *e;
    }
    else
    {
    	maxKey = 0;
    }

    //cout << "Max Key: " << maxKey << endl;
    if (key >= maxKey)
    {
    	int eid = 0;
    	if (keyCount > 0)
    		eid = keyCount;

    	int* entry = (int*) buffer + (2*eid);

        *entry++;
    	*entry = key;
    	entry++;
    	*entry = pid;

     }
    else
    {
        int *intBuffer = (int *) buffer;
    	int *iter = intBuffer + 1; // iterator thorugh keys in buffer
    	int *end = (intBuffer + 2*maxKeysNL) - 1; // pointer to last pageid

    	while (key > *iter)
    		iter += 2;

    	while (end > iter-1)
    	{
    		*end = *(end-2);
    		end--;
    	}


        if(key < *(iter+2))
        {
            *(iter-1) = *(iter+1);
            *iter = key;
            *(iter+1) = pid;
        }
        else // it should not ever reach here, no time to fix for correctness
        {
            *iter = key;
            *(iter-1) = pid;
        }
    }
    if (keyCount == 126) {
        int* p = (int*) buffer + (2*(keyCount+1));
        *p = savedLastPid;
    }
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
	/*int nodeSize = sizeof(PageId) + sizeof(int);
	char* iter = &(buffer[0]);
	int cur = 0;
	int splitter = keyCount/ 2;
	char* splitPoint = iter + (splitter * nodeSize) + sizeof(PageId);// set to midkey
	int i = 0;
	// if (splitter == 0)
		// return RC_NODE_FULL;//actually empty

	iter += sizeof(PageId);
	memcpy(&cur, iter, sizeof(int));

	while(cur < key  && i < keyCount) //iterate the iter until you reach a key that's greater or equal
	{
		i++;
		iter += nodeSize;
		memcpy(&cur, iter, sizeof(int));
	}


	memcpy(sibling.getBuffer(), splitPoint + sizeof(int), sizeof(buffer) - (splitPoint - &(buffer[0])));
	char* splitIter = splitPoint;
	memcpy(&midKey, splitPoint, sizeof(int));

	while (splitIter < &(buffer[PageFile::PAGE_SIZE])) {
		memcpy(splitIter, &NULL_VALUE, sizeof(int));
		splitIter += nodeSize;
	}

	sibling.setKeyCount((keyCount - splitter) - 1); //don't include the midkey
	keyCount = splitter;

	if(key > midKey)
	//if (i > splitter)  //add to the right
		sibling.insert(key, pid);
	else
		insert(key, pid);

	return 0;*/
	if(sibling.getKeyCount() > 0)
        return RC_INVALID_ATTRIBUTE;

    RC rc;
    /*if(rc = insert(key, pid) < 0)
        return rc;*/

    int keyCount = getKeyCount();
    int midPoint = getKeyCount()/2;

    PageId firstofSibling;

    int first = 0;

    /*
     In the for loop below, we need to adjust stop condition based on the number of entries.
     If the number of entries is odd, we want the original non leaf to have 1 more entry
     than its sibling, so we need to stop one position early
    */
    int entrySizeNL = sizeof(int) + sizeof(PageId);
    int pos;
    if(keyCount% 2  == 0)
        pos = 0;
    else
        pos = 1;

    int insertKey;
    PageId insertPid;
    PageId insertPid2;

    int *intBuffer = (int *) buffer;

    for (int k = midPoint; k < keyCount; k++)
    {
        insertPid = *(intBuffer+2*k);
        insertKey = *(intBuffer+2*k+1);
        insertPid2 = *(intBuffer+2*k+2);

        if (first == 0)
        {
            //cout << "Ifalse: " << i << endl;
            first++;
            //cout << "insertPid: " << insertPid << endl;
            //cout << "insertKey: " << insertKey << endl;
            //cout << "insertPid2: " << insertPid2 << endl;
            rc = sibling.initializeRoot(insertPid, insertKey, insertPid2);
            firstofSibling = insertPid;
            //sibling.printContents();
            if (rc < 0)
                return rc;
        }
        else
        {
            //cout << "Itrue: " << i << endl;
            if(rc = sibling.insert(insertKey, insertPid2) < 0)
                return rc;
        }

        memset(buffer+(k*entrySizeNL)+1, -1, entrySizeNL);
    }

    // need to get the new keycount since we removed half the entries

    rc = insert(key, pid);
    //cout << "Insert OG: " << rc << endl;
    //midKey = *(buffer+(getKeyCount()-1)*entrySizeNL-sizeof(int));
    midKey = *(intBuffer+2*(getKeyCount()-1)+1);

    int *end = (int*) buffer;
    end += getKeyCount()*2;
    *end = firstofSibling;


    //cout << "Sibling Key Count: " << sibling.getKeyCount() << endl;

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
			fprintf(stdout, "Obtained pid here");
			memcpy(&pid, iter, sizeof(PageId));
			return 0;
		}
		key++;
		iter += nodeSize;
	}

	iter -= sizeof(PageId);
	fprintf(stdout, "No, I obtained pid here\n");
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
	memmove(iter, &pid2, sizeof(PageId));
	keyCount++;
	return 0;
}


void BTNonLeafNode::printBuffer()
{

	cout << "Print Buffer ===================================\n";
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
		cout << "[" << pid << "]\n";
		// cout << "[" << rid.pid << "]";
		// cout << "[" << rid.sid << "]\n";
	}
	cout << "End Buffer =====================================\n";
}
