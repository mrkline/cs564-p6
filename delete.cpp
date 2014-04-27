#include "catalog.hpp"
#include "query.hpp"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation,
                       const string & attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
	// part 6
	Status status = OK; 
	HeapFileScan *hfs;
	AttrDesc record;
	RID rid;
	Record rec; 
	
	// create a heapfilescan
	hfs = new HeapFileScan(relation, status);
	if(status != OK) {
		return status;
	}
	// if the attrValue is null we have to delete all records in relation
	if(attrValue == NULL) {
		status = hfs -> startScan(0, 0, type, NULL, op);
		if(status != OK) {
			return status;
		}
	}
	else {
		// store the attrDesc for attrName in record
		status = attrCat-> getInfo(relation, attrName, record);
		if(status != OK) {
			return status;
		}
	
	
		int i;
		float f;
	    
		if(type == INTEGER) {
			// if attrValue is an integer cast it to a char
			i = atoi(attrValue);
			attrValue = (char *)&i;
		}
		else if(type == FLOAT) {
			f = atof(attrValue);
			attrValue = (char *)&f;
		}
		// else type is already of type string
		else {
		
		}
		
	
		status = hfs -> startScan(record.attrOffset, record.attrLen, type, attrValue, op);
		if(status != OK ) {
			return status;
		}
	}
	// do the scan
	while(hfs -> scanNext(rid) == OK) {
		// get each record that matches the filter
		// if not record the scan will return all records
		status = hfs -> getRecord(rec);
		if(status != OK) {
			return status;
		}
		
		// then delete the record
		status = hfs -> deleteRecord();
		if( status != OK) {
			return status;
		}
	}
	
	if(status == FILEEOF) {
		return OK;
	}
	delete hfs;
	return status;



}


