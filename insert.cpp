#include "catalog.hpp"
#include "query.hpp"


/*
* Inserts a record into the specified relation.
*
* Returns:
* 	OK on success
* 	an error code otherwise
*/

const Status QU_Insert(const string & relation,
const int attrCnt,
const attrInfo attrList[])
{
	// part 6
	
	Status status;
	InsertFileScan *ifs;
	Record rec;
	RID rid;

	AttrDesc *attrDesc1;
	int attrDescCnt;
	int length =0;
	
	// get the relation info and store it in attrDesc1
	status = attrCat -> getRelInfo(relation, attrDescCnt, attrDesc1);
	if(status != OK) {
		return status;
	}
	
	if(attrDescCnt == attrCnt ) {
		for(int i =0; i < attrDescCnt; i++ ) {
			if(attrList[i].attrValue == NULL) {
				return ATTRTYPEMISMATCH;
			}
			// add to the length variable
			length += attrDesc1[i].attrLen;
		}
	}
	else {
		return ATTRTYPEMISMATCH;
	}	
	// allocate an array of attributes to store data
	char * arrayAttr = (char *) malloc(length);
	if(arrayAttr == NULL ) {
		return INSUFMEM;
	}
		
	// go through all the attribute list
	for(int j = 0; j < attrCnt; j++) {
		for( int k = 0; k< attrDescCnt; k++ ) {
			if(strcmp(attrDesc1[k].attrName, attrList[j].attrName) == 0) {
				if(attrDesc1[k].attrType != attrList[j].attrType) {
					return ATTRTYPEMISMATCH;
				}
					
				// if type is an integer change it to char
				if((Datatype)attrDesc1[k].attrType == INTEGER) {
					int l = atoi((char *)attrList[j].attrValue);
					// copy the attribute description array so we can eventually store it in record.
					memcpy(arrayAttr + attrDesc1[k].attrOffset, 
					&l, attrDesc1[k].attrLen );
				}
					
				// if type float convert to char and copy to array
				else if  ((Datatype) attrDesc1[k].attrType == FLOAT){
					int f = atof((char *)attrList[j].attrValue);
					memcpy(arrayAttr + attrDesc1[k].attrOffset, 
					&f, attrDesc1[k].attrLen );
						
				}
				//  we have type string, copy it
				else {
					memcpy(arrayAttr + attrDesc1[k].attrOffset, 
					(char *)attrList[j].attrValue, attrDesc1[k].attrLen);
				}
			}
		}
	}
	
	// set the record parameter to prepare for insert
	rec.data = arrayAttr;
	rec.length = length;
	
	// initialize a new insert file scan
	ifs = new InsertFileScan(relation, status);
	if(status != OK) { 
		return status; 
	}
	// insert the record into relation.
	status = ifs -> insertRecord(rec, rid);
	if(status != OK) {
		return status;
	}
	
	delete attrDesc1;
	free(arrayAttr);
	return status;
	

}

