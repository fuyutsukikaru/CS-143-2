/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"

using namespace std;

// external functions and variables for load file and sql command parsing
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;
  string value;
  int    count;
  int    diff;

  BTreeIndex index;
  IndexCursor cursor;
  int neCount = 0;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  rc = index.open(table + ".idx", 'r');

  vector<SelCond> keyCond;
  vector<SelCond> valCond;

  int low = -1;
  bool greater_equal = false;

  int high = -1;
  bool less_equal = false;

  if (rc != 0) {
    goto no_index;
  } else {
    for (int i = 0; i < cond.size(); i++) {
      // Check if we have a key condition
      if (cond[i].attr == 1) {
        int keyVal = atoi(cond[i].value);
        switch (condi[i].comp) {
          case SelCond::EQ:
            keyCond.push_back(cond[i]);
            break;
          case SelCond::NE:
            keyCond.push_back(cond[i]);
            neCount++;
            break;
          case SelCond::GT:
            if (keyVal > low || low == -1) {
              low = keyVal;
              greater_equal = false;
            }
            break;
          case SelCond::LT:
            if (keyVal < high || high == -1) {
              high = keyVal;
              less_equal = false;
            }
            break;
          case SelCond::GE:
            if (keyVal >= low || low == -1) {
              low = keyVal;
              greater_equal = true;
            }
            break;
          case SelCond::LE:
            if (keyVal <= high || high == -1) {
              high = keyVal;
              less_equal = true;
            }
            break;
        }
      }
      // Check if we have a value condition
      else if (cond[i].attr == 2) {
        valCond.push_back(cond[i]);
      }
    }
  }
  // Use the index if there are no "not equals"
  if (neCount == 0) {
    // Make sure that there are no conflicts in our conditions
    int prev = -1;
    for (int i = 0; i < keyCond.size(); i++) {

    }

  }

  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
        case 1:
        	diff = key - atoi(cond[i].value);
        	break;
        case 2:
        	diff = strcmp(value.c_str(), cond[i].value);
        	break;
      }

      // skip the tuple if any condition is not met
      switch (cond[i].comp) {
        case SelCond::EQ:
        	if (diff != 0) goto next_tuple;
        	break;
        case SelCond::NE:
        	if (diff == 0) goto next_tuple;
        	break;
        case SelCond::GT:
        	if (diff <= 0) goto next_tuple;
        	break;
        case SelCond::LT:
        	if (diff >= 0) goto next_tuple;
        	break;
        case SelCond::GE:
        	if (diff < 0) goto next_tuple;
        	break;
        case SelCond::LE:
        	if (diff > 0) goto next_tuple;
        	break;
      }
    }

    // the condition is met for the tuple.
    // increase matching tuple counter
    count++;

    // print the tuple
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RC rc = 0;
  RecordFile rf;
  ifstream input;
  string line;

  // Index of our tree
  BTreeIndex dbIndex;

  input.open(loadfile.c_str(), std::ifstream::in);
  if (input.fail()) {
    input.close();
    return RC_FILE_OPEN_FAILED;
  }

  rc = dbIndex.open((table + ".idx").c_str(), 'w');
  if (index && rc != 0) {
    fprintf(stderr, "Error opening index for table %s\n", table.c_str());
    return rc;
  }

  rc = rf.open((table + ".tbl").c_str(), 'w');
  if (rc != 0) {
    fprintf(stderr, "Error in record file for table %s\n", table.c_str());
    return rc;
  }
  if (input.is_open()) {
    while (!input.eof()) {
      getline(input, line);

      if (line == "") {
        break;
      }

      RecordId rid;
      int key;
      string val;

      rc = parseLoadLine(line, key, val);
      if (rc == 0) {
        rc = rf.append(key, val, rid);
        if (rc != 0) {
          fprintf(stderr, "Error appending data to table %s\n", table.c_str());
          break;
        }
        rc = dbIndex.insert(key, rid);
        if (index && rc != 0) {
          fprintf(stderr, "Error inserting into index for table %s\n", table.c_str());
          break;
        }
      } else {
        fprintf(stderr, "Error while parsing loadfile %s\n", loadfile.c_str());
        break;
      }
    }
  }

  try {
    input.close();
  } catch(...) {
    return RC_FILE_CLOSE_FAILED;
  }

  if (rf.close() != 0) {
    return rf.close();
  }

  if (index) {
    rc = dbIndex.close();
  }

  return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;

    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');

    // if there is nothing left, set the value to empty string
    if (c == 0) {
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
